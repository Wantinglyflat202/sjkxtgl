#include "CommandExecutor.h"
#include <iostream>
#include <cstring>
#include <fstream>

void printUsage(const char* programName) {
    std::cout << "Usage: " << programName << " [options]\n";
    std::cout << "Options:\n";
    std::cout << "  -h, --help           Show this help message\n";
    std::cout << "  -b                   Batch processing mode (for automated testing)\n";
    std::cout << "  -d <database>        Specify initial database (USE <database>)\n";
    std::cout << "  -f <path>            Data import: specify file path\n";
    std::cout << "  -t <table>           Data import: specify target table\n";
    std::cout << "  --data <dir>         Set data directory (default: ./data)\n";
    std::cout << "\n";
    std::cout << "Examples:\n";
    std::cout << "  " << programName << "                           # Start interactive mode\n";
    std::cout << "  " << programName << " -b -d mydb                 # Batch mode with database\n";
    std::cout << "  " << programName << " -b -d mydb -f data.csv -t users  # Import data\n";
    std::cout << "  " << programName << " -b < input.sql > output.txt      # Batch with redirection\n";
}

int main(int argc, char* argv[]) {
    std::string dataDir = "./data";
    std::string database;
    std::string importFile;
    std::string importTable;
    bool showHelp = false;
    bool batchMode = false;
    bool initOnly = false;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            showHelp = true;
        } else if (strcmp(argv[i], "-b") == 0) {
            batchMode = true;
        } else if (strcmp(argv[i], "--init") == 0) {
            initOnly = true;
        } else if (strcmp(argv[i], "-d") == 0 && i + 1 < argc) {
            database = argv[++i];
        } else if (strcmp(argv[i], "-f") == 0 && i + 1 < argc) {
            importFile = argv[++i];
        } else if (strcmp(argv[i], "-t") == 0 && i + 1 < argc) {
            importTable = argv[++i];
        } else if (strcmp(argv[i], "--data") == 0 && i + 1 < argc) {
            dataDir = argv[++i];
        } else {
            std::cerr << "Unknown option: " << argv[i] << std::endl;
            printUsage(argv[0]);
            return 1;
        }
    }
    
    if (initOnly) {
        return 0;
    }
    
    if (showHelp) {
        printUsage(argv[0]);
        return 0;
    }
    CommandExecutor executor(dataDir, batchMode);
    if (!database.empty()) {
        std::string result = executor.execute("USE " + database + ";");
        if (!batchMode) {
            std::cout << result;
        } else if (result.find("Error") != std::string::npos) {
            std::cerr << result;
            return 1;
        }
    }
    if (!importFile.empty() && !importTable.empty()) {
        if (database.empty()) {
            std::cerr << "Error: Database must be specified for data import (-d)\n";
            return 1;
        }
        if (!batchMode) {
            std::cerr << "Error: Data import requires batch mode (-b)\n";
            return 1;
        }
        std::string loadCmd = "LOAD DATA INFILE '" + importFile + 
                              "' INTO TABLE " + importTable + 
                              " FIELDS TERMINATED BY ',';";
        std::string result = executor.execute(loadCmd);
        std::cout << result;
        return 0;
    }
    if (batchMode) {
        std::string line;
        std::string currentStatement;
        while (std::getline(std::cin, line)) {
            size_t start = line.find_first_not_of(" \t");
            if (start == std::string::npos) continue;
            if (line[start] == '-' && start + 1 < line.length() && line[start + 1] == '-') continue;
            
            // 特殊处理 exit/quit 命令（无需分号）
            std::string trimmedLine = line.substr(start);
            for (char& c : trimmedLine) c = std::toupper(c);
            if (trimmedLine == "EXIT" || trimmedLine == "QUIT") {
                std::cout << "@\n";
                return 0;
            }
            
            currentStatement += line + " ";
            if (line.find(';') != std::string::npos) {
                std::string result = executor.execute(currentStatement);
                std::cout << result;
                currentStatement.clear();
                if (!executor.isRunning()) return 0;
            }
        }
        if (!currentStatement.empty()) {
            std::string result = executor.execute(currentStatement);
            std::cout << result;
        }
    } else {
        executor.runInteractive();
    }
    return 0;
}
