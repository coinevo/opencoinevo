//
// Created by mwo on 16/12/16.
//

#ifndef RESTBED_EVO_MYSQLACCOUNTS_H
#define RESTBED_EVO_MYSQLACCOUNTS_H

#include "../utils.h"
#include "MySqlConnector.h"



#include <iostream>
#include <memory>



namespace evoeg
{

using namespace mysqlpp;
using namespace std;
using namespace nlohmann;


class EvoTransactionWithOutsAndIns;
class EvoInput;
class EvoOutput;
class EvoTransaction;
class EvoPayment;
class EvoAccount;
class Table;
class CurrentBlockchainStatus;


class MysqlInputs
{

    shared_ptr<MySqlConnector> conn;

public:

    MysqlInputs(shared_ptr<MySqlConnector> _conn);

    bool
    select_for_out(const uint64_t& output_id, vector<EvoInput>& ins);
};



class MysqlOutpus
{

    shared_ptr<MySqlConnector> conn;

public:

    MysqlOutpus(shared_ptr<MySqlConnector> _conn);

    bool
    exist(const string& output_public_key_str, EvoOutput& out);
};



class MysqlTransactions
{

    shared_ptr<MySqlConnector> conn;

public:

    MysqlTransactions(shared_ptr<MySqlConnector> _conn);

    uint64_t
    mark_spendable(const uint64_t& tx_id_no, bool spendable = true);

    uint64_t
    delete_tx(const uint64_t& tx_id_no);

    bool
    exist(const uint64_t& account_id, const string& tx_hash_str, EvoTransaction& tx);

    bool
    get_total_recieved(const uint64_t& account_id, uint64_t& amount);
};

class MysqlPayments
{

    shared_ptr<MySqlConnector> conn;

public:

    MysqlPayments(shared_ptr<MySqlConnector> _conn);

    bool
    select_by_payment_id(const string& payment_id, vector<EvoPayment>& payments);
};


class MySqlAccounts
{

    shared_ptr<MySqlConnector> conn;

    shared_ptr<MysqlTransactions> mysql_tx;

    shared_ptr<MysqlOutpus> mysql_out;

    shared_ptr<MysqlInputs> mysql_in;

    shared_ptr<MysqlPayments> mysql_payment;

    shared_ptr<CurrentBlockchainStatus> current_bc_status;

public:

    MySqlAccounts(shared_ptr<CurrentBlockchainStatus> _current_bc_status);

    MySqlAccounts(shared_ptr<CurrentBlockchainStatus> _current_bc_status,
                  shared_ptr<MySqlConnector> _conn);

    bool
    select(const string& address, EvoAccount& account);

    template <typename T>
    uint64_t
    insert(const T& data_to_insert);

    template <typename T>
    uint64_t
    insert(const vector<T>& data_to_insert);

    /**
     *
     * @tparam T
     * @tparam query_no which query to use, for SELECT_STMT or SELECT_STMT2
     * @param account_id
     * @param selected_data
     * @return
     */
    template <typename T, size_t query_no = 1>
    bool
    select(uint64_t account_id, vector<T>& selected_data);

    template <typename T>
    bool
    update(T const& orginal_row, T const& new_row);

    template <typename T>
    bool
    select_for_tx(uint64_t tx_id, vector<T>& selected_data);

    template <typename T>
    bool
    select_by_primary_id(uint64_t id, T& selected_data);

    bool
    select_txs_for_account_spendability_check(const uint64_t& account_id,
                                              vector<EvoTransaction>& txs);

    bool
    select_inputs_for_out(const uint64_t& output_id, vector<EvoInput>& ins);

    bool
    output_exists(const string& output_public_key_str, EvoOutput& out);

    bool
    tx_exists(const uint64_t& account_id, const string& tx_hash_str, EvoTransaction& tx);

    uint64_t
    mark_tx_spendable(const uint64_t& tx_id_no);

    uint64_t
    mark_tx_nonspendable(const uint64_t& tx_id_no);

    uint64_t
    delete_tx(const uint64_t& tx_id_no);

    bool
    select_payment_by_id(const string& payment_id, vector<EvoPayment>& payments);

    bool
    update_payment(EvoPayment& payment_orginal, EvoPayment& payment_new);

    bool
    get_total_recieved(const uint64_t& account_id, uint64_t& amount);

    void
    disconnect();

    shared_ptr<MySqlConnector>
    get_connection();

    /**
     * DONT use!!!
     *
     * Its only useful in unit tests when you know that nothing will insert
     * any row between calling this and using the returned id
     *
     * @tparam T
     * @param table_class
     * @return
     */
    template <typename T>
    uint64_t
    get_next_primary_id(T&& table_class)
    {
        static_assert(std::is_base_of<Table, std::decay_t<T>>::value, "given class is not Table");

        string sql {"SELECT `auto_increment` FROM INFORMATION_SCHEMA.TABLES WHERE table_name = '"};
        sql += table_class.table_name() + "' AND table_schema = '" +  MySqlConnector::dbname + "'";

        Query query = conn->query(sql);
        query.parse();

        try
        {
            conn->check_if_connected();

            StoreQueryResult  sr = query.store();

            if (!sr.empty())
                return sr[0][0];
        }
        catch (std::exception const& e)
        {
            MYSQL_EXCEPTION_MSG(e);
        }

        return 0;
    }

    // this is useful in unit tests, as we can inject mock CurrentBlockchainStatus
    // after an instance of MySqlAccounts has been created.
    void
    set_bc_status_provider(shared_ptr<CurrentBlockchainStatus> bc_status_provider);

private:
    void _init();
};


}


#endif //RESTBED_EVO_MYSQLACCOUNTS_H
