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
        std::cout << "Metric published to Graphite successfully for topic: " << graphite_topic << " with value: " << value << "\n";
    }

    close(graphite_socket);
    return 0; // Retorna sucesso
}

// PROCESSAMENTO DE DADOS ---------------------------------------------------------------------------------------

void process_sensor_data(const std::string& machine_id, const std::string& sensor_id, const std::string& timestamp, 
const double value, std::deque<double>& sensorData) {
    // Coleta de dados do sensor
    sensorData.push_back(value);

    // Mantenha o tamanho máximo da janela
    if (sensorData.size() > MOVING_AVERAGE_WINDOW) {
        sensorData.pop_front(); // Remova o valor mais antigo se exceder o tamanho da janela
    }

    // Calcular a média móvel do sensor
    if (sensorData.size() > 0) {
        double movingAverage = 0.0;
        for (const auto& data : sensorData) {
            movingAverage += data;
        }
        movingAverage /= sensorData.size();

        std::cout << "Média móvel do uso de " << sensor_id << ": " << movingAverage << std::endl;
        post_metric(machine_id, sensor_id + "_moving_average", timestamp, movingAverage);

        // Detectar outliers usando Z-score
        double zScore = calculateZScore(value, sensorData);
        double zScoreThreshold = 5.0; // Defina o limite de Z-score para considerar um ponto como outlier

        if (std::abs(zScore) > zScoreThreshold) {
            // Se o valor atual for um outlier
            std::cout << "Outlier detectado para o uso da CPU: " << value << std::endl;
            post_metric(machine_id, "alarms." + sensor_id + "_outlier", timestamp, 1);
        } else {
            // Se não for um outlier
            std::cout << "Uso normal de " << sensor_id << ": " << value << std::endl;
        }
    }
}

void process_sensor_alarm(const std::string& machine_id, const std::string& sensor_id, const std::string& timestamp, const double value) {
    // Suponhamos que os dados do sensor chegam a cada 30 segundos
    const int expected_interval_seconds = 30;
    
    // Lógica para alarme de inatividade
    static std::unordered_map<std::string, int> sensor_last_timestamps;
    std::string sensor_key = machine_id + "-" + sensor_id;

    auto now = std::chrono::system_clock::now();
    auto received_time = std::chrono::system_clock::from_time_t(string_to_time_t(timestamp));
    int seconds_since_last_timestamp = std::chrono::duration_cast<std::chrono::seconds>(now - received_time).count();
    std::cout << "\n\n" << "--------------------->    Análise de dados para o sensor " << sensor_id << "   <---------------------\n";
    std::cout << "Hora atual: " << timestamp << "\n";
    if (seconds_since_last_timestamp > expected_interval_seconds * 10) {
        // Gerar alarme de inatividade
        std::cout << "Alarme de inatividade para o sensor " << sensor_id << " da máquina " << machine_id << std::endl;
        post_metric(machine_id, "alarms.inactive", timestamp, 1); // Enviar alarme para o Graphite
    }
    // Exemplo: Cálculos de média móvel, detecção de outliers, análise de tendências, etc.
}

void process_sensor_data_cpu(const std::string& machine_id, const std::string& sensor_id, const std::string& timestamp, const double value) {
    static std::deque<double> cpuUsageData; // Use uma deque para armazenar valores anteriores
    process_sensor_data(machine_id, sensor_id, timestamp, value, cpuUsageData);
}

void process_sensor_data_mem(const std::string& machine_id, const std::string& sensor_id, const std::string& timestamp, const double value) {
    static std::deque<double> memUsageData; // Use uma deque para armazenar valores anteriores
    process_sensor_data(machine_id, sensor_id, timestamp, value, memUsageData);
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

            post_metric(machine_id, sensor_id, timestamp, value);
            process_sensor_alarm(machine_id, sensor_id, timestamp, value);
            if (sensor_id == "cpu_usage") {
                process_sensor_data_cpu(machine_id, sensor_id, timestamp, value);
            } else {
                 process_sensor_data_mem(machine_id, sensor_id, timestamp, value);
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
