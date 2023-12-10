#include <iostream>
#include <cstdlib>
#include <chrono>
#include <thread>
#include <unistd.h>
#include "json.hpp"
#include "mqtt/client.h"
#include <string>
#include <vector>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <deque>
#include <cmath>
#include <ctime>

#define QOS 1
#define BROKER_ADDRESS "tcp://localhost:1883"
#define GRAPHITE_HOST "127.0.0.1"
#define GRAPHITE_PORT 2003

// CÁLCULO E CONVERSÃO -------------------------------------------------------------------------------------------

std::time_t string_to_time_t(const std::string& time_string) {
    std::tm tm = {};
    std::istringstream ss(time_string);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
    return std::mktime(&tm);
}

std::tm string_to_tm(const std::string& timestamp) {
    std::tm tm = {};
    std::istringstream ss(timestamp);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    return tm;
}

// Defina o tamanho da janela para a média móvel
const size_t MOVING_AVERAGE_WINDOW = 5; // Por exemplo, uma janela de tamanho 5

// Função para calcular a média móvel de um conjunto de valores
double calculateMovingAverage(const std::deque<double>& values) {
    if (values.empty()) {
        return 0.0; // Se a fila estiver vazia, retorna 0 como média móvel
    }

    double sum = 0.0;
    for (const auto& value : values) {
        sum += value;
    }

    return sum / static_cast<double>(values.size());
}


// Função para calcular o Z-score de um valor em relação a um conjunto de dados
double calculateZScore(double value, const std::deque<double>& values) {
    if (values.empty()) {
        return 0.0; // Se a fila estiver vazia, retorna 0 como Z-score
    }

    double mean = calculateMovingAverage(values);
    double variance = 0.0;

    for (const auto& val : values) {
        variance += std::pow(val - mean, 2);
    }

    variance /= static_cast<double>(values.size());
    double stdDeviation = std::sqrt(variance);

    if (stdDeviation == 0.0) {
        return 0.0; // Retorna 0 se o desvio padrão for zero para evitar divisão por zero
    }

    return (value - mean) / stdDeviation;
}

// Função para calcular a tendência usando regressão linear simples
double calculateTrend(const std::deque<double>& values) {
    size_t n = values.size();
    double sumX = 0.0, sumY = 0.0, sumXY = 0.0, sumXX = 0.0;

    if (n < 2) {
        return 0.0;
    }

    for (size_t i = 0; i < n; ++i) {
        sumX += i + 1;
        sumY += values[i];
        sumXY += (i + 1) * values[i];
        sumXX += (i + 1) * (i + 1);
    }

    double slope = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX);

    return slope;
}

// POSTAR MÉTRICA ------------------------------------------------------------------------------------------

int post_metric(const std::string& machine_id, const std::string& sensor_id, const std::string& timestamp_str, const double value) {
    int graphite_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (graphite_socket == -1) {
        std::cerr << "Error: Failed to create socket\n";
        return -1; // Retorna um código de erro
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(GRAPHITE_PORT);
    if (inet_pton(AF_INET, GRAPHITE_HOST, &server_addr.sin_addr) <= 0) {
        std::cerr << "Error: Invalid address or address not supported\n";
        close(graphite_socket);
        return -1; // Retorna um código de erro
    }

    if (connect(graphite_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) == -1) {
        std::cerr << "Error: Failed to connect to Graphite\n";
        close(graphite_socket);
        return -1; // Retorna um código de erro
    }

    std::string graphite_topic = "machines." + machine_id + "." + sensor_id;
    std::stringstream metric_stream;
    metric_stream << graphite_topic << " " << value << " " << string_to_time_t(timestamp_str) << "\n";

    std::string metric_data = metric_stream.str();
    ssize_t sent_bytes = send(graphite_socket, metric_data.c_str(), metric_data.size(), 0);
    if (sent_bytes == -1) {
        std::cerr << "Error: Failed to send metric data for topic: " << graphite_topic << " with value: " << value << "\n";
        close(graphite_socket);
        return -1; // Retorna um código de erro
    } else {
        //std::cout << "Metric published to Graphite successfully for topic: " << graphite_topic << " with value: " << value << "\n";
    }

    close(graphite_socket);
    return 0; // Retorna sucesso
}

// PROCESSAMENTO DE DADOS ---------------------------------------------------------------------------------------

void process_sensor_data(const std::string& machine_id, const std::string& sensor_id, const std::string& timestamp, 
const double value, std::deque<double>& sensorData, std::deque<std::string>& sensorTimestamp) {

    std::cout << "\n\n" << "--------------------->    Análise de dados para o sensor " << sensor_id << "   <---------------------\n";
    std::tm time = string_to_tm(timestamp);
    std::cout << "Data: " << std::put_time(&time, "%d/%m/%Y, Hora: %H:%M:%S") << std::endl;
     std::cout << "ID da máquina: " << machine_id << std::endl;

    // Coleta de dados do sensor
    sensorData.push_back(value);
    sensorTimestamp.push_back(timestamp);

    // Mantenha o tamanho máximo da janela
    if (sensorData.size() > MOVING_AVERAGE_WINDOW) {
        sensorData.pop_front(); // Remova o valor mais antigo se exceder o tamanho da janela
        sensorTimestamp.pop_front();
    }

    // Calcular a média móvel do sensor
    if (sensorData.size() > 0) {
        double movingAverage = 0.0;
        for (const auto& data : sensorData) {
            movingAverage += data;
        }
        movingAverage /= sensorData.size();

        std::cout << "Média móvel do uso de " << sensor_id << ": " << movingAverage << std::endl;
        post_metric(machine_id, sensor_id + "." + sensor_id + "_moving_average", timestamp, movingAverage);

        // Detectar outliers usando Z-score
        double zScore = calculateZScore(value, sensorData);
        double zScoreThreshold = 1.0; // Defina o limite de Z-score para considerar um ponto como outlier

        if (std::abs(zScore) > zScoreThreshold) {
            // Se o valor atual for um outlier
            std::cout << "[ALARME] Outlier detectado: " << value << std::endl;
            post_metric(machine_id, "alarms." + sensor_id + "_outlier", timestamp, 1);
        } else {
            // Se não for um outlier
            std::cout << "Uso normal de " << sensor_id << ": " << value << std::endl;
        }
        // Calcular a tendência dos valores
        double trend = calculateTrend(sensorData);
        std::cout << "Tendência do uso de " << sensor_id << ": " << trend << std::endl;
        post_metric(machine_id, sensor_id + "." + sensor_id + "_trend", timestamp, trend);
        std::cout << "----------------------------------------------------------------------------------------------\n";
    }
}

void process_sensor_alarm(const std::string& machine_id, const std::string& sensor_id, const std::deque<std::string>* sensorTimestamps) {
    const int expected_interval_seconds = 30; // intervalo de chegada esperado
    const int max_expected_delay = expected_interval_seconds; // máximo de atraso esperado para gerar um alarme

    if (sensorTimestamps != nullptr && !sensorTimestamps->empty()) {
        std::string last_timestamp = sensorTimestamps->back();
        std::time_t last_time = string_to_time_t(last_timestamp);
        std::time_t current_time = std::time(nullptr);
        int seconds_since_last_timestamp = current_time - last_time;
        auto now = std::chrono::system_clock::now();
        std::time_t now_c = std::chrono::system_clock::to_time_t(now);
        std::tm* now_tm = std::gmtime(&now_c);
        std::stringstream ss;
        ss << std::put_time(now_tm, "%FT%TZ");
        std::string timestamp = ss.str();

        if (seconds_since_last_timestamp > max_expected_delay) {
            // Gerar alarme se o atraso for maior do que o esperado
            std::cout << "[ALARME] Dados do sensor " << sensor_id << " da máquina " << machine_id << " não foram recebidos por mais de 10 períodos de tempo previstos.\n";
            post_metric(machine_id, "alarms.inactive_" + sensor_id, timestamp, 1);
        }
    }
}

std::deque<std::string> cpuUsageTimestamps;
void process_sensor_data_cpu(const std::string& machine_id, const std::string& sensor_id, const std::string& timestamp, const double value) {
    static std::deque<double> cpuUsageData; // Use uma deque para armazenar valores anteriores
    process_sensor_data(machine_id, sensor_id, timestamp, value, cpuUsageData, cpuUsageTimestamps);
}

std::deque<std::string> memUsageTimestamps;
void process_sensor_data_mem(const std::string& machine_id, const std::string& sensor_id, const std::string& timestamp, const double value) {
    static std::deque<double> memUsageData; // Use uma deque para armazenar valores anteriores
    process_sensor_data(machine_id, sensor_id, timestamp, value, memUsageData, memUsageTimestamps);
}

std::vector<std::string> split(const std::string &str, char delim) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(str);
    while (std::getline(tokenStream, token, delim)) {
        tokens.push_back(token);
    }
    return tokens;
}


std::mutex mtx;
std::vector<std::string> monitored_sensors;

void check_sensor_inactivity(const std::string& machine_id, const std::string& sensor_id, const std::deque<std::string>* sensorTimestamps) {
    // Função para verificar a inatividade do sensor
    while (true) {
        {
            std::lock_guard<std::mutex> lock(mtx);
            process_sensor_alarm(machine_id, sensor_id, sensorTimestamps);
        }
        std::this_thread::sleep_for(std::chrono::seconds(20));
    }
}

void add_monitored_sensor(const std::string& sensor_id) {
    std::lock_guard<std::mutex> lock(mtx);
    monitored_sensors.push_back(sensor_id);
}

bool is_sensor_monitored(const std::string& sensor_id) {
    std::lock_guard<std::mutex> lock(mtx);
    return std::find(monitored_sensors.begin(), monitored_sensors.end(), sensor_id) != monitored_sensors.end();
}

void process_message(const std::string& machine_id, const std::string& sensor_id, const std::deque<std::string>* sensorTimestamps) {
    if (!is_sensor_monitored(sensor_id)) {
        std::thread sensor_thread(check_sensor_inactivity, machine_id, sensor_id, sensorTimestamps);
        sensor_thread.detach();
        add_monitored_sensor(sensor_id);
    }
}

// MAIN ---------------------------------------------------------------------------------------------------------

int main(int argc, char* argv[]) {
    std::string clientId = "clientId";
    mqtt::async_client client(BROKER_ADDRESS, clientId);

    // Create an MQTT callback.
    class callback : public virtual mqtt::callback {
    public:

        void message_arrived(mqtt::const_message_ptr msg) override {
            auto j = nlohmann::json::parse(msg->get_payload());

            std::string topic = msg->get_topic();
            auto topic_parts = split(topic, '/');
            std::string machine_id = topic_parts[2];
            std::string sensor_id = topic_parts[3];

            std::string timestamp = j["timestamp"];
            double value = j["value"];

            post_metric(machine_id, sensor_id + "." + sensor_id, timestamp, value);
            if (sensor_id == "cpu_usage") {
                process_sensor_data_cpu(machine_id, sensor_id, timestamp, value);
                process_message(machine_id, sensor_id, &cpuUsageTimestamps);
            } else {
                 process_sensor_data_mem(machine_id, sensor_id, timestamp, value);
                 process_message(machine_id, sensor_id, &memUsageTimestamps);
            }
        }
    };

    callback cb;
    client.set_callback(cb);
    // Connect to the MQTT broker.
    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);

    try {
        client.connect(connOpts)->wait();
        client.subscribe("/sensors/#", QOS);
        std::cout << "Subscrided\n";
    } catch (mqtt::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return EXIT_SUCCESS;
}
