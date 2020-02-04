//
// Created by mwo on 7/01/17.
//

#define MYSQLPP_SSQLS_NO_STATICS 1

#include "MySqlAccounts.h"
//#include "TxSearch.h"
#include "../CurrentBlockchainStatus.h"

#include "ssqlses.h"

namespace evoeg
{


MysqlInputs::MysqlInputs(shared_ptr<MySqlConnector> _conn)
        : conn {_conn}
{}

bool
MysqlInputs::select_for_out(const uint64_t& output_id,
                            vector<EvoInput>& ins)
{
    try
    {
        conn->check_if_connected();

        Query query = conn->query(EvoInput::SELECT_STMT4);
        query.parse();

        query.storein(ins, output_id);

        return !ins.empty();
    }
    catch (std::exception const& e)
    {
        MYSQL_EXCEPTION_MSG(e);
        //throw  e;
    }

    return false;
}


MysqlOutpus::MysqlOutpus(shared_ptr<MySqlConnector> _conn): conn {_conn}
{}



bool
MysqlOutpus::exist(const string& output_public_key_str, EvoOutput& out)
{
    try
    {
        conn->check_if_connected();

        Query query = conn->query(EvoOutput::EXIST_STMT);
        query.parse();

        vector<EvoOutput> outs;

        query.storein(outs, output_public_key_str);

        if (outs.empty())
            return false;

        out = std::move(outs.at(0));

    }
    catch (std::exception const& e)
    {
        MYSQL_EXCEPTION_MSG(e);
        return false;
    }

    return true;
}


MysqlTransactions::MysqlTransactions(shared_ptr<MySqlConnector> _conn)
    : conn {_conn}
{}

uint64_t
MysqlTransactions::mark_spendable(const uint64_t& tx_id_no, bool spendable)
{
    try
    {
        conn->check_if_connected();

        Query query = conn->query(
                    spendable ?
                      EvoTransaction::MARK_AS_SPENDABLE_STMT
                                : EvoTransaction::MARK_AS_NONSPENDABLE_STMT);
        query.parse();


        SimpleResult sr = query.execute(tx_id_no);

        return sr.rows();
    }
    catch (std::exception const& e)
    {
        MYSQL_EXCEPTION_MSG(e);
        //throw  e;
    }

    return 0;
}

uint64_t
MysqlTransactions::delete_tx(const uint64_t& tx_id_no)
{
    try
    {
        conn->check_if_connected();

        Query query = conn->query(EvoTransaction::DELETE_STMT);
        query.parse();

        SimpleResult sr = query.execute(tx_id_no);

        return sr.rows();
    }
    catch (std::exception const& e)
    {
        MYSQL_EXCEPTION_MSG(e);
        //throw  e;
    }

    return 0;
}


bool
MysqlTransactions::exist(const uint64_t& account_id,
                         const string& tx_hash_str, EvoTransaction& tx)
{
    try
    {
        conn->check_if_connected();

        Query query = conn->query(EvoTransaction::EXIST_STMT);
        query.parse();

        vector<EvoTransaction> outs;

        query.storein(outs, account_id, tx_hash_str);

        if (outs.empty())
            return false;

        tx = outs.at(0);

    }
    catch (std::exception const& e)
    {
        MYSQL_EXCEPTION_MSG(e);
        return false;
    }

    return true;
}


bool
MysqlTransactions::get_total_recieved(const uint64_t& account_id,
                                      uint64_t& amount)
{
    try
    {
        conn->check_if_connected();

        Query query = conn->query(EvoTransaction::SUM_EVO_RECIEVED);
        query.parse();

        StoreQueryResult sqr = query.store(account_id);

        if (!sqr.empty())
        {
            amount = sqr.at(0)["total_received"];
            return true;
        }
    }
    catch (std::exception const& e)
    {
        MYSQL_EXCEPTION_MSG(e);
    }

    return false;
}

MysqlPayments::MysqlPayments(shared_ptr<MySqlConnector> _conn): conn {_conn}
{}

bool
MysqlPayments::select_by_payment_id(const string& payment_id,
                                    vector<EvoPayment>& payments)
{

    try
    {
        conn->check_if_connected();

        Query query = conn->query(EvoPayment::SELECT_STMT2);
        query.parse();

        payments.clear();
        query.storein(payments, payment_id);

        return !payments.empty();
    }
    catch (std::exception const& e)
    {
        MYSQL_EXCEPTION_MSG(e);
        //throw  e;
    }

    return false;
}

MySqlAccounts::MySqlAccounts(
        shared_ptr<CurrentBlockchainStatus> _current_bc_status)
    : current_bc_status {_current_bc_status}
{
    // create connection to the mysql
    conn = make_shared<MySqlConnector>(
            new mysqlpp::ReconnectOption(true));

    _init();
}

MySqlAccounts::MySqlAccounts(
        shared_ptr<CurrentBlockchainStatus> _current_bc_status,
        shared_ptr<MySqlConnector> _conn)
    : current_bc_status {_current_bc_status}
{
    conn = _conn;

    _init();
}


bool
MySqlAccounts::select(const string& address, EvoAccount& account)
{
    try
    {
        conn->check_if_connected();

        Query query = conn->query(EvoAccount::SELECT_STMT2);
        query.parse();

        vector<EvoAccount> res;
        query.storein(res, address);

        //while(query.more_results());

        if (!res.empty())
        {
            account = std::move(res.at(0));
            return true;
        }

    }
    catch (std::exception const& e)
    {
        MYSQL_EXCEPTION_MSG(e);
        //throw  e;
    }

    return false;
}

template <typename T>
uint64_t
MySqlAccounts::insert(const T& data_to_insert)
{
    try
    {
        conn->check_if_connected();

        Query query = conn->query();

        query.insert(data_to_insert);

        SimpleResult sr = query.execute();

        if (sr.rows() == 1)
            return sr.insert_id();

    }
    catch (std::exception const& e)
    {
        MYSQL_EXCEPTION_MSG(e);;
    }

    return 0;
}

// Explicitly instantiate insert template for our tables
template
uint64_t MySqlAccounts::insert<EvoAccount>(const EvoAccount& data_to_insert);
template
uint64_t MySqlAccounts::insert<EvoTransaction>(
                    const EvoTransaction& data_to_insert);
template
uint64_t MySqlAccounts::insert<EvoOutput>(const EvoOutput& data_to_insert);
template
uint64_t MySqlAccounts::insert<EvoInput>(const EvoInput& data_to_insert);
template
uint64_t MySqlAccounts::insert<EvoPayment>(const EvoPayment& data_to_insert);

template <typename T>
uint64_t
MySqlAccounts::insert(const vector<T>& data_to_insert)
{
    try
    {
        conn->check_if_connected();

        Query query = conn->query();

        query.insert(data_to_insert.begin(), data_to_insert.end());

        SimpleResult sr = query.execute();

        return sr.rows();

    }
    catch (std::exception const& e)
    {
        MYSQL_EXCEPTION_MSG(e);
    }

    return 0;
}

// Explicitly instantiate insert template for our tables
template
uint64_t MySqlAccounts::insert<EvoOutput>(
        const vector<EvoOutput>& data_to_insert);

template
uint64_t MySqlAccounts::insert<EvoInput>(
        const vector<EvoInput>& data_to_insert);

template <typename T, size_t query_no>
bool
MySqlAccounts::select(uint64_t account_id, vector<T>& selected_data)
{
    try
    {
        conn->check_if_connected();

        Query query = conn->query((query_no == 1
                                   ? T::SELECT_STMT : T::SELECT_STMT2));
        query.parse();

        selected_data.clear();

        query.storein(selected_data, account_id);

        // this is confusing. So I get false from this method
        // when this is empty and when there is some exception!
        return !selected_data.empty();
        //return true;
    }
    catch (std::exception const& e)
    {
        MYSQL_EXCEPTION_MSG(e);
    }

    return false;
}

template
bool MySqlAccounts::select<EvoAccount>(uint64_t account_id,
        vector<EvoAccount>& selected_data);

template
bool MySqlAccounts::select<EvoTransaction>(uint64_t account_id,
        vector<EvoTransaction>& selected_data);

template
bool MySqlAccounts::select<EvoOutput>(uint64_t account_id,
        vector<EvoOutput>& selected_data);

template // this will use SELECT_STMT2 which selectes
        // based on transaction id, not account_id,
bool MySqlAccounts::select<EvoOutput, 2>(uint64_t tx_id,
        vector<EvoOutput>& selected_data);

template
bool MySqlAccounts::select<EvoInput>(uint64_t account_id,
        vector<EvoInput>& selected_data);

template
bool MySqlAccounts::select<EvoPayment>(uint64_t account_id,
        vector<EvoPayment>& selected_data);

template // this will use SELECT_STMT2 which selectes
         // based on transaction id, not account_id,
bool MySqlAccounts::select<EvoInput, 2>(uint64_t tx_id,
        vector<EvoInput>& selected_data);


template <typename T>
bool
MySqlAccounts::update(T const& orginal_row, T const& new_row)
{
    try
    {
        conn->check_if_connected();

        Query query = conn->query();

        query.update(orginal_row, new_row);

        SimpleResult sr = query.execute();

        return sr.rows() == 1;
    }
    catch (std::exception const& e)
    {
        MYSQL_EXCEPTION_MSG(e);
    }

    return false;
}

template
bool MySqlAccounts::update<EvoAccount>(
        EvoAccount const& orginal_row, EvoAccount const& new_row);

template
bool MySqlAccounts::update<EvoPayment>(
        EvoPayment const& orginal_row, EvoPayment const& new_row);

template <typename T>
bool
MySqlAccounts::select_for_tx(uint64_t tx_id, vector<T>& selected_data)
{
    return select<T, 2>(tx_id, selected_data);
}

template // this will use SELECT_STMT2 which selectes based on
         // transaction id, not account_id,
bool MySqlAccounts::select_for_tx<EvoOutput>(uint64_t tx_id,
        vector<EvoOutput>& selected_data);


template // this will use SELECT_STMT2 which selectes
         //based on transaction id, not account_id,
bool MySqlAccounts::select_for_tx<EvoInput>(uint64_t tx_id,
        vector<EvoInput>& selected_data);

template <typename T>
bool
MySqlAccounts::select_by_primary_id(uint64_t id, T& selected_data)
{
     try
    {
        conn->check_if_connected();

        Query query = conn->query(T::SELECT_STMT3);
        query.parse();

        vector<T> outs;

        query.storein(outs, id);

        if (!outs.empty())
        {
            selected_data = std::move(outs.at(0));
            return true;
        }
    }
    catch (std::exception const& e)
    {
        MYSQL_EXCEPTION_MSG(e);
        //throw  e;
    }

    return false;
}

//template
//bool MySqlAccounts::select_by_primary_id<EvoTransaction>(
//                           uint64_t id, EvoTransaction& selected_data);

template
bool MySqlAccounts::select_by_primary_id<EvoInput>(
        uint64_t id, EvoInput& selected_data);

template
bool MySqlAccounts::select_by_primary_id<EvoOutput>(
        uint64_t id, EvoOutput& selected_data);

template
bool MySqlAccounts::select_by_primary_id<EvoPayment>(
        uint64_t id, EvoPayment& selected_data);

bool
MySqlAccounts::select_txs_for_account_spendability_check(
        const uint64_t& account_id, vector<EvoTransaction>& txs)
{

    for (auto it = txs.begin(); it != txs.end(); )
    {
        // first we check if txs stored in db are already spendable
        // it means if they are older than 10 blocks. If  yes,
        // we mark them as spendable, as we assume that blocks
        // older than 10 blocks are permanent, i.e, they wont get
        // orphaned.

        EvoTransaction& tx = *it;

        if (bool {tx.spendable} == false)
        {

            if (current_bc_status->is_tx_unlocked(tx.unlock_time, tx.height))
            {
                // this tx was before marked as unspendable, but now
                // it is spendable. Meaning, that its older than 10 blocks.
                // so mark it as spendable in mysql, so that its permanet.

                uint64_t no_row_updated = mark_tx_spendable(tx.id.data);

                if (no_row_updated != 1)
                {
                    cerr << "no_row_updated != 1 due to  "
                            "evo_accounts->mark_tx_spendable(tx.id)\n";
                    return false;
                }

                tx.spendable = true;
            }
            else
            {
                // tx was marked as non-spendable, i.e., younger than 10 blocks
                // so we still are going to use this txs, but we need to double
                // check if its still valid, i.e., it's block did not get orphaned.
                // we do this by checking if txs still exists in the blockchain
                // and if its blockchain_tx_id is same as what we have in our mysql.

                uint64_t blockchain_tx_id {0};

                current_bc_status->tx_exist(tx.hash, blockchain_tx_id);

                if (blockchain_tx_id != tx.blockchain_tx_id)
                {
                    // tx does not exist in blockchain, or its blockchain_id
                    // changed
                    // for example, it was orhpaned, and then readded.

                    uint64_t no_row_updated = delete_tx(tx.id.data);

                    if (no_row_updated != 1)
                    {
                        cerr << "no_row_updated != 1 due to  "
                                "evo_accounts->delete_tx(tx.id)\n";
                        return false;
                    }

                    // because txs does not exist in blockchain anymore,
                    // we assume its back to mempool, and it will be rescanned
                    // by tx search thread once added again to some block.

                    // so we remove it from txs vector
                    it = txs.erase(it);
                    continue;
                }

                // set unlock_time field so that frontend displies it
                // as a locked tx, if unlock_time is zero.
                // coinbtase txs have this set already. regular tx
                // have unlock_time set to zero by default, but they cant
                // be spent anyway.

                if (tx.unlock_time == 0)
                    tx.unlock_time = tx.height
                            + CRYPTONOTE_DEFAULT_TX_SPENDABLE_AGE;

            } // else

        } // if (bool {tx.spendable} == false)

        ++it;

    } // for (auto it = txs.begin(); it != txs.end(); )

    return true;
}



bool
MySqlAccounts::select_inputs_for_out(const uint64_t& output_id,
                                     vector<EvoInput>& ins)
{
    return mysql_in->select_for_out(output_id, ins);
}

bool
MySqlAccounts::output_exists(const string& output_public_key_str,
                             EvoOutput& out)
{
    return mysql_out->exist(output_public_key_str, out);
}

bool
MySqlAccounts::tx_exists(const uint64_t& account_id,
                         const string& tx_hash_str, EvoTransaction& tx)
{
    return mysql_tx->exist(account_id, tx_hash_str, tx);
}

uint64_t
MySqlAccounts::mark_tx_spendable(const uint64_t& tx_id_no)
{
    return mysql_tx->mark_spendable(tx_id_no);
}

uint64_t
MySqlAccounts::mark_tx_nonspendable(const uint64_t& tx_id_no)
{
    return mysql_tx->mark_spendable(tx_id_no, false);
}

uint64_t
MySqlAccounts::delete_tx(const uint64_t& tx_id_no)
{
    return mysql_tx->delete_tx(tx_id_no);
}

bool
MySqlAccounts::select_payment_by_id(const string& payment_id,
                                    vector<EvoPayment>& payments)
{
    return mysql_payment->select_by_payment_id(payment_id, payments);
}

bool
MySqlAccounts::get_total_recieved(const uint64_t& account_id,
                                  uint64_t& amount)
{
    return mysql_tx->get_total_recieved(account_id, amount);
}

void
MySqlAccounts::disconnect()
{
    get_connection()->get_connection().disconnect();
}


shared_ptr<MySqlConnector>
MySqlAccounts::get_connection()
{
    return conn;
}


void
MySqlAccounts::set_bc_status_provider(
        shared_ptr<CurrentBlockchainStatus> bc_status_provider)
{
    current_bc_status = bc_status_provider;
}

void
MySqlAccounts::_init()
{

    // use same connection when working with other tables
    mysql_tx        = make_shared<MysqlTransactions>(conn);
    mysql_out       = make_shared<MysqlOutpus>(conn);
    mysql_in        = make_shared<MysqlInputs>(conn);
    mysql_payment   = make_shared<MysqlPayments>(conn);
}

}
