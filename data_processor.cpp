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

#define QOS 1
#define BROKER_ADDRESS "tcp://localhost:1883"
#define GRAPHITE_HOST "graphite"
#define GRAPHITE_PORT 2003

// Função para persistir a métrica no Graphite usando o protocolo Carbon
void post_metric(const std::string& machine_id, const std::string& sensor_id, const std::string& timestamp_str, const int value) {
    int graphite_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (graphite_socket == -1) {
        std::cerr << "Error: Failed to create socket\n";
        return;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(GRAPHITE_PORT);
    inet_pton(AF_INET, GRAPHITE_HOST, &server_addr.sin_addr);

    if (connect(graphite_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) == -1) {
        std::cerr << "Error: Failed to connect to Graphite\n";
        close(graphite_socket);
        return;
    }

    std::stringstream metric_stream;
    metric_stream << machine_id << "." << sensor_id << " " << value << " " << timestamp_str << "\n";

    std::string metric_data = metric_stream.str();
    ssize_t sent_bytes = send(graphite_socket, metric_data.c_str(), metric_data.size(), 0);
    if (sent_bytes == -1) {
        std::cerr << "Error: Failed to send metric data\n";
    }

    close(graphite_socket);
}

void process_sensor_data(const std::string& machine_id, const std::string& sensor_id, const std::string& timestamp, const int value) {
    // Aqui você pode realizar o processamento dos dados do sensor
    // Exemplo: Verificar se houve inatividade por um período específico
    
    // Suponhamos que os dados do sensor chegam a cada 30 segundos
    const int expected_interval_seconds = 30;
    
    // Lógica para alarme de inatividade
    static std::unordered_map<std::string, int> sensor_last_timestamps;
    std::string sensor_key = machine_id + "-" + sensor_id;

    auto now = std::chrono::system_clock::now();
    auto received_time = std::chrono::system_clock::from_time_t(std::stoi(timestamp));
    int seconds_since_last_timestamp = std::chrono::duration_cast<std::chrono::seconds>(now - received_time).count();

    if (seconds_since_last_timestamp > expected_interval_seconds * 10) {
        // Gerar alarme de inatividade
        std::cout << "Alarme de inatividade para o sensor " << sensor_id << " da máquina " << machine_id << std::endl;
        post_metric(machine_id, "alarms.inactive", timestamp, 1); // Enviar alarme para o Graphite
    }

    // Processamento personalizado
    // Aqui você pode realizar qualquer processamento adicional conforme necessário
    // Exemplo: Cálculos de média móvel, detecção de outliers, análise de tendências, etc.
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

int main(int argc, char* argv[]) {
    std::string clientId = "DataProcessor";
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
            int value = j["value"];

            // Persistir métrica no Graphite
            post_metric(machine_id, sensor_id, timestamp, value);

            // Processar os dados do sensor
            process_sensor_data(machine_id, sensor_id, timestamp, value);
        }
    };

    callback cb;
    client.set_callback(cb);

    // Connect to the MQTT broker and subscribe to the topic for sensor monitors
    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);
    
    try {
        client.connect(connOpts);
        client.subscribe("/sensors/#", QOS);
    } catch (mqtt::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return EXIT_SUCCESS;
}
