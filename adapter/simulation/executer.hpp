#pragma once

#if defined(HFT_BASE_NOWQP)
#error WEAK_QUEUE_PRIORITY required, to fix remove -DHFT_BASE_NOWQP from args
#endif

#include <hft/base/munit.hpp>
#include <hft/mantle/mktdata.hpp>
#include <hft/mantle/order.hpp>
#include <hft/mantle/array.hpp>
#include <hft/mantle/indicator.hpp>
#include <unordered_map>
#include <hft/mantle/store.hpp>

namespace hft
{
    namespace adapter
    {
        namespace simulation
        {
            class Executer: public base::MUnit<Executer>
            {
                HFT_GRAMMAR_ENUM(inputs, mktdata, order_send, nsefo_exchtp);
                using INPUT_TYPES = std::tuple< mantle::MktData<>, mantle::Order, mantle::Array<double> >;

                HFT_GRAMMAR_ENUM(outputs, order_recv);
                using OUTPUT_TYPES = std::tuple< mantle::Order >;

                HFT_BASE_TUNIT;

            public:
                Executer(boost::python::dict kwargs);
                inline grammar::datetime get_time()
                    {return grammar::datetime::max();};
                void go_ahead();
                void activate(InputId input_channel_id);
            private:
                long fill_limit_orders(size_t inst_id, double price, long size, mantle::Side side);
                long fill_early_orders(size_t inst_id, double price, long size, mantle::Side side, uint64_t trade_wqp);
            private:
                base::Log log_;
                grammar::timedelta network_delay_;
                grammar::timedelta name_coeff_;
                grammar::timedelta feed_coeff_;
                grammar::timedelta queued_threshold_;
                grammar::timedelta queued_coeff_;
                grammar::timedelta exchange_delay_;
                grammar::timedelta exchange_delay_cap_;

                uint64_t n_orders_;
                uint64_t n_trades_;
                double epsilon_;
                std::unordered_map<size_t, mantle::Order> open_;
                std::vector<std::multimap<double, size_t, std::greater<double>>> price_map_[2];
                std::vector<std::unordered_map<double, size_t>> touch_map_[2];
                HFT_GRAMMAR_ENUM(delay_phase, NETWORK , EXCHANGE, QUEUED);
                using delay_tuple = std::tuple<grammar::datetime, mantle::Order, delay_phase>;
                struct timestamp_compare
                {
                    constexpr bool operator()(const delay_tuple& a, const delay_tuple& b) const
                    {
                        return std::get<0>(a) > std::get<0>(b);
                    }
                };
                std::priority_queue<delay_tuple, std::vector<delay_tuple>, timestamp_compare> delay_queue_;
                mantle::Exchange exchange_;
                HFT_GRAMMAR_ENUM(FillModel, BOOK_CROSS, LEVEL_LAST, LEVEL_FIRST, ALWAYS, LEVEL_FIRST_NOREPEAT, LEVEL_LAST_NOREPEAT, QP) fill_model_;

                struct CrossMaker
                {
                    uint64_t oid;
                    mantle::Side side;
                    bool uncrossed_by_trade;

                    //below 2 fields relvant only when uncrossed by trade
                    double uncrosser_trade_px;
                    double remaining_qty;
                };
                std::vector<CrossMaker> crossmaker;
                std::vector<size_t> top_fill_size;
                std::vector<uint64_t> old_wqp[2];
                size_t max_queue_size_;
                bool uncross_book_;

                std::unordered_map<int, std::unordered_map<int, int>> feed_counter;
                std::unordered_map<int,int> name_counter;
                std::unordered_map<int,int> order_map_feed; //order_id vs initial_feed_num
                std::unordered_map<int,int> order_map_name; //order_id vs initial_name_num
                //std::unordered_map<uint64_t, grammar::datetime> feed_exchange_time;
                std::unordered_map<uint64_t, uint64_t> feed_exchange_time;
                std::unordered_map<uint64_t, long int> feed_order_id;
                std::unordered_map<uint64_t, long int> feed_md_count;
                std::unordered_map<uint64_t, bool> feed_queued;
                std::unordered_map<int,int> id_exchange_map;
                std::unordered_map<int,int> id_feed_map;
                std::unordered_map<int, std::unordered_map<int, grammar::datetime>> md_time;
            };
        }
    }
}
