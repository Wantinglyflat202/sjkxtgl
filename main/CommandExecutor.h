#ifndef COMMAND_EXECUTOR_H
#define COMMAND_EXECUTOR_H

#include "../parser/ANTLRParser.h"
#include "../system/SystemManager.h"
#include "../query/QueryExecutor.h"
#include <string>
#include <memory>
class CommandExecutor {
private:
    std::unique_ptr<FileManager> fileManager;
    std::unique_ptr<BufPageManager> bufPageManager;
    std::unique_ptr<SystemManager> systemManager;
    std::unique_ptr<QueryExecutor> queryExecutor;
    SimpleParser parser;
    bool running;
    bool batchMode; 
    ResultSet executeDDL(const SQLStatement& stmt);
    
    ResultSet executeDML(const SQLStatement& stmt);
 
    ResultSet executeAlter(const SQLStatement& stmt);

    std::string formatInteractive(const ResultSet& result);
    std::string formatBatch(const ResultSet& result);
    std::string formatDescBatch(const TableMeta& meta);
    std::string formatDescInteractive(const TableMeta& meta);
    
public:

    CommandExecutor(const std::string& dataDir = "./data", bool batch = false);
    ~CommandExecutor();
    std::string execute(const std::string& sql);

    std::string executeFile(const std::string& filePath);
    void runInteractive();
    bool isRunning() const { return running; }
    void stop() { running = false; }
    std::string getCurrentDatabase() const;

    void flush();

    bool isBatchMode() const { return batchMode; }
};

#endif 
