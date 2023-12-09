#include <iostream>
#include <cstdlib>
#include <chrono>
#include <ctime>
#include <thread>
#include <unistd.h>
#include "json.hpp" // json handling
#include "mqtt/client.h" // paho mqtt
#include <iomanip>
#include <sys/sysinfo.h> // Para obter informações de memória
#include <unistd.h>
#include <fstream>
#include <sstream>

#define QOS 1
#define BROKER_ADDRESS "tcp://localhost:1883"
#define MESSAGE_INTERVAL 10 // Intervalo inicial da mensagem (em segundos)

// Definição da estrutura de dados para um sensor
struct SensorInfo {
    std::string sensor_id;
    std::string data_type;
    int data_interval;
};

// Função para obter a porcentagem de CPU utilizada
float getCPUUsage() {
    std::ifstream fileStat("/proc/stat");
    if (!fileStat.is_open()) {
        std::cerr << "Erro ao abrir /proc/stat\n";
        return -1;
    }

    std::string line;
    std::getline(fileStat, line);
    std::istringstream ss(line);

    // Ler a primeira linha que contém informações sobre a CPU
    std::string cpuLabel;
    ss >> cpuLabel;

    // Ler os valores referentes ao uso da CPU
    int user, nice, system, idle;
    ss >> user >> nice >> system >> idle;

    // Calcular o total do tempo de CPU
    int totalCpuTime = user + nice + system + idle;

    // Calcular o tempo da CPU utilizada (exceto idle)
    int cpuUsed = totalCpuTime - idle;

    // Calcular a porcentagem de uso da CPU
    float cpuUsage = ((float)cpuUsed / totalCpuTime) * 100;

    return cpuUsage;
}

// Função para obter a porcentagem de memória utilizada
float getMemoryUsage() {
    struct sysinfo memInfo;
    if (sysinfo(&memInfo) != -1) {
        long long totalVirtualMem = memInfo.totalram;
        totalVirtualMem += memInfo.totalswap;
        totalVirtualMem *= memInfo.mem_unit;

        long long virtualMemUsed = memInfo.totalram - memInfo.freeram;
        virtualMemUsed += memInfo.totalswap - memInfo.freeswap;
        virtualMemUsed *= memInfo.mem_unit;

        float memoryPercentage = ((float)virtualMemUsed / totalVirtualMem) * 100;
        return memoryPercentage;
    }
    return -1; // Indicando erro ao obter informações de memória
}

// Função para enviar a mensagem inicial do SensorMonitor
void sendInitialMessage(mqtt::client& client, const std::string& machineId, const std::vector<SensorInfo>& sensors) {
    nlohmann::json initialMessage;

    initialMessage["machine_id"] = machineId;
    initialMessage["sensors"] = nlohmann::json::array();

    for (const auto& sensor : sensors) {
        nlohmann::json sensorJson;
        sensorJson["sensor_id"] = sensor.sensor_id;
        sensorJson["data_type"] = sensor.data_type;
        sensorJson["data_interval"] = sensor.data_interval;

        initialMessage["sensors"].push_back(sensorJson);
    }

    mqtt::message msg("/sensor_monitors", initialMessage.dump(), QOS, false);
    client.publish(msg);
    std::clog << "Initial message published: " << initialMessage.dump() << std::endl;
}


int main(int argc, char* argv[]) {
    std::string clientId = "sensor-monitor";
    mqtt::client client(BROKER_ADDRESS, clientId);

    // Connect to the MQTT broker.
    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);

    try {
        client.connect(connOpts);
    } catch (mqtt::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    std::clog << "connected to the broker" << std::endl;

    // Get the unique machine identifier, in this case, the hostname.
    char hostname[1024];
    gethostname(hostname, 1024);
    std::string machineId(hostname);

    // Definição dos sensores a serem monitorados
    std::vector<SensorInfo> sensors = {
        {"cpu_usage", "float", 5000}, // Sensor de uso da CPU
        {"memory_usage", "float", 5000} // Sensor de uso de memória
    };

    // Enviar a mensagem inicial
    sendInitialMessage(client, machineId, sensors);

    while (true) {
        // Get the current time in ISO 8601 format in UTC.
        auto now = std::chrono::system_clock::now();
        std::time_t now_c = std::chrono::system_clock::to_time_t(now);
        std::tm* now_tm = std::gmtime(&now_c); // Using gmtime to get UTC time

        std::stringstream ss;
        ss << std::put_time(now_tm, "%FT%TZ");
        std::string timestamp = ss.str();

        // Get sensor readings
        float cpuUsage = getCPUUsage();
        float memoryUsage = getMemoryUsage();

        // Publish CPU usage to MQTT
        nlohmann::json cpuJson;
        cpuJson["timestamp"] = timestamp;
        cpuJson["value"] = cpuUsage;

        std::string cpuTopic = "/sensors/" + machineId + "/cpu_usage";
        mqtt::message cpuMsg(cpuTopic, cpuJson.dump(), QOS, false);
        client.publish(cpuMsg);
        std::clog << "CPU message published - topic: " << cpuTopic << " - message: " << cpuJson.dump() << std::endl;

        // Publish memory usage to MQTT
        nlohmann::json memoryJson;
        memoryJson["timestamp"] = timestamp;
        memoryJson["value"] = memoryUsage;

        std::string memoryTopic = "/sensors/" + machineId + "/memory_usage";
        mqtt::message memoryMsg(memoryTopic, memoryJson.dump(), QOS, false);
        client.publish(memoryMsg);
        std::clog << "Memory message published - topic: " << memoryTopic << " - message: " << memoryJson.dump() << std::endl;

        // Sleep for some time.
        std::this_thread::sleep_for(std::chrono::seconds(MESSAGE_INTERVAL)); // Adjust the interval as needed
    }

    return EXIT_SUCCESS;
}
