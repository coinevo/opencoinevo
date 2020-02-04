cmake_minimum_required(VERSION 3.10)

project(myevo)

set(SOURCE_FILES
		CmdLineOptions.cpp
		CurrentBlockchainStatus.cpp
        db/MySqlConnector.cpp
        db/MySqlAccounts.cpp
        db/ssqlses.cpp
		OpenCoinevoRequests.cpp
		TxSearch.cpp
        RPCCalls.cpp
		omversion.h.in
		BlockchainSetup.cpp
		ThreadRAII.cpp
        db/MysqlPing.cpp
        TxUnlockChecker.cpp
        utils.cpp
        RandomOutputs.cpp)

add_library(myevo STATIC
    ${SOURCE_FILES})

target_link_libraries(myevo 
    PUBLIC
    EVOEG::core 
    ${restbed_LIBRARY}
    ${MYSQLPP_LIBRARIES})

target_include_directories(myevo 
    PUBLIC
    ${restbed_INCLUDE}
    ${MYSQLPP_INCLUDE_DIRS})
