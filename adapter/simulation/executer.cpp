#include "executer.hpp"

using namespace hft::adapter::simulation;
using namespace hft::mantle;
namespace py = boost::python;
using namespace std;

DECLARE_HFT_BASE_UNIT(Executer);

Executer::Executer(py::dict kwargs)
{
    kwargs.setdefault("log", "");
    log_.open(py::extract<string>(kwargs["log"])());
    log_.writeln("date,time,order_id,tag,name,side,order_price,order_size,bid,ask,bid_sz,ask_sz,filled_size");

    network_delay_ = py::extract<grammar::timedelta>(kwargs["delay"])();

    if (kwargs.has_key("feed_coeff"))
    {
        feed_coeff_ = py::extract<grammar::timedelta>(kwargs["feed_coeff"])();
    }
    else
    {
        feed_coeff_ = grammar::timedelta::zero();
    }

    if (kwargs.has_key("name_coeff"))
    {
        name_coeff_ = py::extract<grammar::timedelta>(kwargs["name_coeff"])();
    }
    else
    {
        name_coeff_ = grammar::timedelta::zero();
    }
    
    if (kwargs.has_key("queued_threshold"))
    {
        queued_threshold_ = py::extract<grammar::timedelta>(kwargs["queued_threshold"])();
    }
    else
    {
        queued_threshold_ = grammar::timedelta::zero();
    }
    
    if (kwargs.has_key("queued_coeff"))
    {
        queued_coeff_ = py::extract<grammar::timedelta>(kwargs["queued_coeff"])();
    }
    else
    {
        queued_coeff_ = grammar::timedelta::zero();
    }

    if (kwargs.has_key("exchange_delay"))
    {
        exchange_delay_ = py::extract<grammar::timedelta>(kwargs["exchange_delay"])();
    }
    else
    {
        exchange_delay_ = grammar::timedelta::zero();
    }

    if (kwargs.has_key("exchange_delay_cap"))
    {
        exchange_delay_cap_ = py::extract<grammar::timedelta>(kwargs["exchange_delay_cap"])();
    }
    else
    {
        exchange_delay_cap_ = grammar::timedelta::zero();
    }
    
    if (queued_coeff_ == grammar::timedelta::zero())
        queued_threshold_ = grammar::timedelta::zero();

    kwargs.setdefault("epsilon", 1e-8);
    epsilon_ = py::extract<double>(kwargs["epsilon"])();
    n_orders_ = 0;
    n_trades_ = 0;
    CrossMaker cm;
    cm.oid = 0;
    cm.uncrossed_by_trade = false;
    crossmaker.resize(core.n_instruments, cm);
    top_fill_size.resize(core.n_instruments,0);
    old_wqp[Side::BUY()].resize(core.n_instruments,0);
    old_wqp[Side::SELL()].resize(core.n_instruments,0);
    price_map_[Side::BUY()].resize(core.n_instruments);
    price_map_[Side::SELL()].resize(core.n_instruments);
    touch_map_[Side::BUY()].resize(core.n_instruments);
    touch_map_[Side::SELL()].resize(core.n_instruments);
    kwargs.setdefault("fill_model", (string) FillModel::BOOK_CROSS());
    fill_model_ = py::extract<string>(kwargs["fill_model"])();

    kwargs.setdefault("exchange", (string) Exchange::COMMON());
    exchange_ = py::extract<string>(kwargs["exchange"])();

    kwargs.setdefault("max_queue_size", std::numeric_limits<size_t>::max());
    max_queue_size_ = py::extract<size_t>(kwargs["max_queue_size"])();

    kwargs.setdefault("uncross_book", true);
    uncross_book_ = py::extract<bool>(kwargs["uncross_book"])();

}

void Executer::go_ahead()
{
    assert(!delay_queue_.empty());

    auto& order_send = std::get<1>(delay_queue_.top());
    auto& delay_phase = std::get<2>(delay_queue_.top());
    auto& delay_que_tp = std::get<0>(delay_queue_.top());
    
    if(delay_phase == delay_phase::QUEUED())
    {
        int order_id = order_send.order_id;
        uint64_t key = uint64_t(id_exchange_map[order_send.inst_id]) << 32 | uint64_t(id_feed_map[order_send.inst_id]);
        grammar::timedelta add_delay_extra  = queued_coeff_ * feed_queued[key];

        auto tp = delay_que_tp.combine(add_delay_extra);
        base::core.event_queue.push(make_tuple(tp, this));
        if (feed_queued[key])
            delay_queue_.push(make_tuple(tp, order_send, delay_phase::QUEUED()));
        else
            delay_queue_.push(make_tuple(tp, order_send, delay_phase::EXCHANGE()));
        delay_queue_.pop();
        return; 
    }

    auto& exchange_last_time = md_time[id_exchange_map[order_send.inst_id]][id_feed_map[order_send.inst_id]];

    if(delay_phase == delay_phase::NETWORK())
    {
        if (order_send.deadline.combine_uncapped(exchange_delay_) > exchange_last_time and order_send.type == Order::Type::IOC())
        {
            log_.writeln(std::forward_as_tuple(delay_que_tp,order_send.order_id, "deadline",core[order_send.inst_id].name,order_send.side,order_send.price,order_send.size,order_send.deadline,exchange_last_time,0));
            if (order_send.deadline.combine_uncapped(exchange_delay_) - exchange_last_time > exchange_delay_cap_)
            {
                const_cast<Order *>(&order_send)->deadline = exchange_last_time.combine_uncapped(exchange_delay_cap_ - exchange_delay_);
                log_.writeln(std::forward_as_tuple(delay_que_tp,order_send.order_id, "cap deadline",core[order_send.inst_id].name,order_send.side,order_send.price,order_send.size,order_send.deadline,exchange_last_time,0));    
            }
            
            grammar::timedelta add_delay_extra = max((order_send.deadline - exchange_last_time)/2, grammar::timedelta::microsecond() * 5);
	        // grammar::timedelta add_delay_extra = grammar::timedelta::microsecond() * 5;
            
            auto tp = delay_que_tp.combine(add_delay_extra);
            base::core.event_queue.push(make_tuple(tp, this));
            delay_queue_.push(make_tuple(tp, order_send, delay_phase::NETWORK()));
            delay_queue_.pop();
            return;
        }

        int order_id = order_send.order_id;

        int feed_diff = feed_counter[id_exchange_map[order_send.inst_id]][id_feed_map[order_send.inst_id]] - order_map_feed[order_send.order_id];
        int name_diff = name_counter[order_send.inst_id] - order_map_name[order_send.order_id];

        uint64_t key = uint64_t(id_exchange_map[order_send.inst_id]) << 32 | uint64_t(id_feed_map[order_send.inst_id]);
        grammar::timedelta add_delay_extra  = feed_coeff_*feed_diff + name_coeff_*name_diff + queued_coeff_ * feed_queued[key];

        auto tp = delay_que_tp.combine(add_delay_extra);
        base::core.event_queue.push(make_tuple(tp, this));
        if (feed_queued[key])
            delay_queue_.push(make_tuple(tp, order_send, delay_phase::QUEUED()));
        else
            delay_queue_.push(make_tuple(tp, order_send, delay_phase::EXCHANGE()));
        delay_queue_.pop();
        return;
    }

    assert(delay_phase == delay_phase::EXCHANGE());
    assert(order_send.state == Order::State::OUT());
    switch(order_send.event)
    {
        case Order::Event::ADD():
        {
            auto exchange_order_id = n_orders_++;
            Order order;
            order.copy_from(order_send);
            order.state = Order::State::DONE();
            order.filled_size = 0;
            order.exchange_order_id = exchange_order_id;
            order.wqp = mutable_core.event_counter++;
            order.exchange_last_time = exchange_last_time;
            
            auto& order_recv = get_output<outputs::order_recv()>();
            order_recv.copy_from(order);
            commit<outputs::order_recv()>();

            auto& instrument = core[order.inst_id];
            double first_leg_price = (instrument.type == InstrumentType::SPREAD()) ? core[instrument.sell_leg_id].get_ticker().last_trade_price : NAN;
            assert(!(instrument.type == InstrumentType::SPREAD() and std::isnan(first_leg_price)));

            if (fill_model_ == FillModel::ALWAYS())
            {
                order.event           = Order::Event::TRADE();
                order.last_trade_size  = order.size;
                order.last_trade_price = order.price;
                order.last_trade_id    = n_trades_++;
                order.filled_size     += order.last_trade_size;

                if (instrument.type == InstrumentType::SPREAD())
                {
                    {
                        auto& order_recv = get_output<outputs::order_recv()>();
                        order_recv.copy_from(order);
                        order_recv.inst_id          = instrument.sell_leg_id;
                        order_recv.side             = !order.side;
                        order_recv.last_trade_price = first_leg_price;
                        commit<outputs::order_recv()>();
                    }

                    {
                        auto& order_recv = get_output<outputs::order_recv()>();
                        order_recv.copy_from(order);
                        order_recv.inst_id          = instrument.buy_leg_id;
                        order_recv.side             = order.side;
                        order_recv.last_trade_price = first_leg_price + order.price;
                        commit<outputs::order_recv()>();
                    }
                }
                else
                {
                    auto& order_recv = get_output<outputs::order_recv()>();
                    order_recv.copy_from(order);
                    commit<outputs::order_recv()>();
                }
            }
            else
            {
                auto& book = core[order_send.inst_id].get_book();
                size_t start_rank[Side::size()] = {0};
                int start_size[Side::size()] = {0};
                if (uncross_book_)
                {
                    while(book.px(start_rank[Side::BUY()], Side::BUY()) >= book.px(start_rank[Side::SELL()], Side::SELL()))
                    {
                        auto min_size = std::min(
                            book.qty(start_rank[Side::BUY()], Side::BUY()) - start_size[Side::BUY()],
                            book.qty(start_rank[Side::SELL()], Side::SELL()) - start_size[Side::SELL()]
                        );
                        assert(min_size > 0);
                        for (size_t s=0; s < Side::size(); s++)
                        {
                            if (book.qty(start_rank[s], Side(s)) - start_size[s] == min_size)
                            {
                                start_rank[s]++;
                                start_size[s] = 0;
                            }
                            else
                            {
                                start_size[s] += min_size;
                            }
                        }
                    }
                }

                for(size_t i=start_rank[!order.side]; i<book.n_levels(!order.side) && order.filled_size < order.size; i++)
                {
                    auto level_price = book.px(i, !(order.side));
                    auto level_size = book.qty(i, !(order.side));
                    if ((order.side == Side::BUY() && level_price > order.price + epsilon_) ||
                        (order.side == Side::SELL() && level_price < order.price - epsilon_))
                        break;

                    order.event           = Order::Event::TRADE();
                    int tag = 0;
                    if (fill_model_ == FillModel::LEVEL_LAST_NOREPEAT() || fill_model_== FillModel::LEVEL_FIRST_NOREPEAT())
                    {
                        if(i==start_rank[!order.side])
                        {
                            if(touch_map_[!order.side][order.inst_id].find(level_price) != touch_map_[!order.side][order.inst_id].end())
                            {
                                // unsigned int size1  = min((unsigned) level_size - start_size[!order.side], order.size - order.filled_size);
                                // order.last_trade_size = min((unsigned)touch_map_[!order.side][order.inst_id][level_price], size1);
                                // touch_map_[!order.side][order.inst_id][level_price] = touch_map_[!order.side][order.inst_id][level_price] - (order.last_trade_size);
                                // tag = 1;
                                // if (order.last_trade_size == 0)
                                // {
                                //     tag =4;
                                //     continue;
                                // }
                                unsigned int size1  = min((unsigned) level_size - start_size[!order.side], order.size - order.filled_size);
                                order.last_trade_size = ((unsigned)touch_map_[!order.side][order.inst_id][level_price] > size1) ? 0 : size1 - (unsigned)touch_map_[!order.side][order.inst_id][level_price];
                                tag = 1;
                                if (order.last_trade_size == 0)
                                {
                                    tag =4;
                                    log_.writeln(forward_as_tuple(delay_que_tp,order.order_id,tag,core[order.inst_id].name,order.side,order.price,order.size,book.px(0, Side::BUY()),book.px(0, Side::SELL()),book.qty(0, Side::BUY()),book.qty(0, Side::SELL()),touch_map_[!order.side][order.inst_id][order.price]));
                                    continue;
                                }
                            }
                            else
                            {
                                order.last_trade_size  = min((unsigned) level_size - start_size[!order.side], order.size - order.filled_size);
                                tag =2;
                            }
                            touch_map_[!order.side][order.inst_id][level_price] +=  (order.last_trade_size);
                        }
                        else
                        {
                            order.last_trade_size  = min((unsigned) level_size, order.size - order.filled_size);
                            tag=3;
                        }
                        log_.writeln(forward_as_tuple(delay_que_tp,order.order_id,tag,core[order.inst_id].name,order.side,order.price,order.size,book.px(0, Side::BUY()),book.px(0, Side::SELL()),book.qty(0, Side::BUY()),book.qty(0, Side::SELL()),touch_map_[!order.side][order.inst_id][order.price]));
                    }
                    else
                    {
                        if(i==start_rank[!order.side])
                            order.last_trade_size  = min((unsigned) level_size - start_size[!order.side], order.size - order.filled_size);
                        else
                            order.last_trade_size  = min((unsigned) level_size, order.size - order.filled_size);
                    }
                    order.last_trade_price = level_price;
                    order.last_trade_id    = n_trades_++;
                    order.filled_size     += order.last_trade_size;

                    if (instrument.type == InstrumentType::SPREAD())
                    {
                        {
                            auto& order_recv = get_output<outputs::order_recv()>();
                            order_recv.copy_from(order);
                            order_recv.inst_id          = instrument.sell_leg_id;
                            order_recv.side             = !order.side;
                            order_recv.last_trade_price = first_leg_price;
                            commit<outputs::order_recv()>();
                        }

                        {
                            auto& order_recv = get_output<outputs::order_recv()>();
                            order_recv.copy_from(order);
                            order_recv.inst_id          = instrument.buy_leg_id;
                            order_recv.side             = order.side;
                            order_recv.last_trade_price = first_leg_price + order.price;
                            commit<outputs::order_recv()>();
                        }
                    }
                    else
                    {
                        auto& order_recv = get_output<outputs::order_recv()>();
                        order_recv.copy_from(order);
                        commit<outputs::order_recv()>();
                    }
                }
            }

            if (order_send.type == Order::Type::IOC() or order.filled_size == order.size)
            {
                order.event = Order::Event::CANCEL();
                auto& order_recv = get_output<outputs::order_recv()>();
                order_recv.copy_from(order);
                commit<outputs::order_recv()>();
            }
            else
            {
                open_[order.exchange_order_id].copy_from(order);
                auto& price_map = price_map_[order.side][order.inst_id];
                price_map.emplace((order.side == Side::BUY()) ? order.price : -order.price, order.exchange_order_id);
            }
            break;
        }
        case Order::Event::CANCEL():
        {
            size_t exchange_order_id = order_send.exchange_order_id;
            auto it = open_.find(exchange_order_id);
            if(it == open_.cend())
            {
                //send rejection
                auto& order_recv = get_output<outputs::order_recv()>();
                order_recv.copy_from(order_send);
                order_recv.state = Order::State::FAILED();
                commit<outputs::order_recv()>();
                break;
            }
            auto& order = it->second;
            if (order_send.order_id != order.order_id)
            {
                //send rejection
                //break;
            }
            if (order_send.price != order.price)
            {
                //send rejection
                //break;
            }
            if (order_send.display_size != order.display_size)
            {
                //send rejection
                //break;
            }
            if (order_send.size != order.size)
            {
                //send rejection
                //break;
            }
            if (order_send.inst_id != order.inst_id)
            {
                //send rejection
                //break;
            }
            if (order_send.side != order.side)
            {
                //send rejection
                //break;
            }
            if (order_send.filled_size <= order.size)
            {
                //send rejection
                //break;
            }
            order.event = Order::Event::CANCEL();
            auto& order_recv = get_output<outputs::order_recv()>();
            order_recv.copy_from(order);
            order_recv.exchange_last_time = exchange_last_time;
            commit<outputs::order_recv()>();

            {
                auto& price_map = price_map_[order.side][order.inst_id];
                auto it = price_map.find(order.side == Side::BUY() ? order.price : -order.price);
                while(it->second != order.exchange_order_id)
                {
                    assert(it != price_map.end());
                    it++;
                }
                price_map.erase(it);
            }
            open_.erase(it);
            break;
        }
        case Order::Event::MODIFY():
        {
            size_t exchange_order_id = order_send.exchange_order_id;
            auto it = open_.find(exchange_order_id);
            if(it == open_.cend())
            {
                //send rejection
                auto& order_recv = get_output<outputs::order_recv()>();
                order_recv.copy_from(order_send);
                order_recv.state = Order::State::FAILED();
                order_recv.exchange_last_time = exchange_last_time;
                commit<outputs::order_recv()>();
                break;
            }
            auto& order = it->second;
            if (order_send.price != order.price || order_send.size != order.size)
            {
                auto& order_recv = get_output<outputs::order_recv()>();
                order_recv.copy_from(order_send);
                order_recv.state = Order::State::REJECTED();
                order_recv.exchange_last_time = exchange_last_time;
                commit<outputs::order_recv()>();
                break;
            }
            if (order_send.filled_size != order.filled_size)
            {
                auto& order_recv = get_output<outputs::order_recv()>();
                order_recv.copy_from(order_send);
                order_recv.state = Order::State::FAILED();
                order_recv.exchange_last_time = exchange_last_time;
                commit<outputs::order_recv()>();
                break;
            }
            if (order_send.order_id != order.order_id)
            {
                //send rejection
                //break;
            }
            if (order_send.price != order.price)
            {
                //send rejection
                //break;
            }
            if (order_send.display_size != order.display_size)
            {
                //send rejection
                //break;
            }
            if (order_send.size != order.size)
            {
                //send rejection
                //break;
            }
            if (order_send.inst_id != order.inst_id)
            {
                //send rejection
                //break;
            }
            if (order_send.side != order.side)
            {
                //send rejection
                //break;
            }
            if (order.filled_size > order_send.new_size)
            {
                //send rejection
                //break;
            }
            if(order.price != order_send.new_price || order_send.new_size > order.size)
            {
                order.wqp = mutable_core.event_counter++;
            }
            auto old_price = order.price;
            order.price = order_send.new_price;
            order.display_size = order_send.new_display_size;
            order.size = order_send.new_size;
            order.event = Order::Event::MODIFY();
            auto& order_recv = get_output<outputs::order_recv()>();
            order_recv.copy_from(order);
            order_recv.exchange_last_time = exchange_last_time;
            commit<outputs::order_recv()>();

            if (order_send.price != order_send.new_price)
            {
                //fill if new_price is more aggressive
                if ((order.side == Side::BUY() && order.price > old_price + epsilon_) ||
                    (order.side == Side::SELL() && order.price < old_price - epsilon_))
                {
                    auto& book = core[order_send.inst_id].get_book();

                    auto& instrument = core[order.inst_id];
                    double first_leg_price = (instrument.type == InstrumentType::SPREAD()) ? core[instrument.sell_leg_id].get_ticker().last_trade_price : NAN;
                    assert(!(instrument.type == InstrumentType::SPREAD() and std::isnan(first_leg_price)));

                    size_t start_rank[Side::size()] = {0};
                    int start_size[Side::size()] = {0};
                    if (uncross_book_)
                    {
                        while(book.px(start_rank[Side::BUY()], Side::BUY()) >= book.px(start_rank[Side::SELL()], Side::SELL()))
                        {
                            auto min_size = std::min(
                                book.qty(start_rank[Side::BUY()], Side::BUY()) - start_size[Side::BUY()],
                                book.qty(start_rank[Side::SELL()], Side::SELL()) - start_size[Side::SELL()]
                            );
                            assert(min_size > 0);
                            for (size_t s=0; s < Side::size(); s++)
                            {
                                if (book.qty(start_rank[s], Side(s)) - start_size[s] == min_size)
                                {
                                    start_rank[s]++;
                                    start_size[s] = 0;
                                }
                                else
                                {
                                    start_size[s] += min_size;
                                }
                            }
                        }
                    }

                    for(size_t i=start_rank[!order.side]; i<book.n_levels(!order.side) && order.filled_size < order.size; i++)
                    {
                        const auto& level_price = book.px(i, !(order.side));
                        const auto& level_size = book.qty(i, !(order.side));
                        if ((order.side == Side::BUY() && level_price > order.price + epsilon_) ||
                            (order.side == Side::SELL() && level_price < order.price - epsilon_))
                            break;

                        if ((order.side == Side::BUY() && level_price > old_price + epsilon_) ||
                            (order.side == Side::SELL() && level_price < old_price - epsilon_))
                        {
                            order.event           = Order::Event::TRADE();
                            if(i==start_rank[!order.side])
                                order.last_trade_size  = min((unsigned) level_size - start_size[!order.side], order.size - order.filled_size);
                            else
                                order.last_trade_size  = min((unsigned) level_size, order.size - order.filled_size);
                            order.last_trade_price = level_price;
                            order.last_trade_id    = n_trades_++;
                            order.filled_size     += order.last_trade_size;

                            if (instrument.type == InstrumentType::SPREAD())
                            {
                                {
                                    auto& order_recv = get_output<outputs::order_recv()>();
                                    order_recv.copy_from(order);
                                    order_recv.inst_id          = instrument.sell_leg_id;
                                    order_recv.side             = !order.side;
                                    order_recv.last_trade_price = first_leg_price;
                                    commit<outputs::order_recv()>();
                                }

                                {
                                    auto& order_recv = get_output<outputs::order_recv()>();
                                    order_recv.copy_from(order);
                                    order_recv.inst_id          = instrument.buy_leg_id;
                                    order_recv.side             = order.side;
                                    order_recv.last_trade_price = first_leg_price + order.price;
                                    commit<outputs::order_recv()>();
                                }
                            }
                            else
                            {
                                auto& order_recv = get_output<outputs::order_recv()>();
                                order_recv.copy_from(order);
                                commit<outputs::order_recv()>();
                            }
                        }
                    }
                }

                if (order.filled_size == order.size
                    or order_send.price > order_send.new_price + epsilon_
                    or order_send.price < order_send.new_price - epsilon_)
                {
                    auto& price_map = price_map_[order.side][order.inst_id];
                    auto it = price_map.find((order.side == Side::BUY()) ? order_send.price : -order_send.price);
                    while(it->second != order.exchange_order_id)
                    {
                        assert(it != price_map.end());
                        it++;
                    }
                    price_map.erase(it);

                    if (order.filled_size == order.size)
                    {
                        order.event = Order::Event::CANCEL();
                        auto& order_recv = get_output<outputs::order_recv()>();
                        order_recv.copy_from(order);
                        commit<outputs::order_recv()>();

                        open_.erase(it->second);
                    }
                    else
                    {
                        price_map.emplace((order.side == Side::BUY()) ? order.price : -order.price, order.exchange_order_id);
                    }
                }
            }

            break;
        }
        break;
        default:
        //TODO: throw error?
        break;
    }

    delay_queue_.pop();
}
//give fill to orders with wqp less than trade_wqp OR which are better priced.
long Executer::fill_early_orders(size_t inst_id, double price, long size, Side side, uint64_t trade_wqp)
{
    long filled = 0;
    auto& price_map = price_map_[side][inst_id];
    while(filled < size)
    {
        auto it = price_map.cbegin();
        if (it == price_map.cend() or it->first < (side == Side::BUY() ? price : -price) - epsilon_ )
            break;

        auto it2 = open_.find(it->second);
        assert(it2 != open_.end());
        auto& order = it2->second;
        bool px_match = (std::abs(order.price - price) < epsilon_) ? true : false;
        //px either matches or our order is aggresively priced.
        if(px_match && order.wqp > trade_wqp)
            break;

        order.event           = Order::Event::TRADE();
        order.last_trade_size  = min((unsigned) (size - filled), order.size - order.filled_size);
        order.last_trade_price = order.price;
        order.last_trade_id    = n_trades_++;
        order.filled_size     += order.last_trade_size;
        filled += order.last_trade_size;

        auto& instrument = core[order.inst_id];
        double first_leg_price = (instrument.type == InstrumentType::SPREAD()) ? core[instrument.sell_leg_id].get_ticker().last_trade_price : NAN;
        assert(!(instrument.type == InstrumentType::SPREAD() and std::isnan(first_leg_price)));

        if (instrument.type == InstrumentType::SPREAD())
        {
            {
                auto& order_recv = get_output<outputs::order_recv()>();
                order_recv.copy_from(order);
                order_recv.inst_id          = instrument.sell_leg_id;
                order_recv.side             = !order.side;
                order_recv.last_trade_price = first_leg_price;
                commit<outputs::order_recv()>();
            }

            {
                auto& order_recv = get_output<outputs::order_recv()>();
                order_recv.copy_from(order);
                order_recv.inst_id          = instrument.buy_leg_id;
                order_recv.side             = order.side;
                order_recv.last_trade_price = first_leg_price + order.price;
                commit<outputs::order_recv()>();
            }
        }
        else
        {
            auto& order_recv = get_output<outputs::order_recv()>();
            order_recv.copy_from(order);
            commit<outputs::order_recv()>();
        }

        if (order.filled_size == order.size)
        {
            order.event = Order::Event::CANCEL();
            auto& order_recv = get_output<outputs::order_recv()>();
            order_recv.copy_from(order);
            commit<outputs::order_recv()>();

            open_.erase(it->second);
            price_map.erase(it);
        }
    }
    return filled;
}
long Executer::fill_limit_orders(size_t inst_id, double price, long size, Side side)
{
    long filled = 0;
    auto& price_map = price_map_[side][inst_id];
    while(filled < size)
    {
        auto it = price_map.cbegin();
        if (it == price_map.cend() or it->first < (side == Side::BUY() ? price : -price) - epsilon_)
            break;

        auto it2 = open_.find(it->second);
        assert(it2 != open_.end());
        auto& order = it2->second;

        order.event           = Order::Event::TRADE();
        order.last_trade_size  = min((unsigned) (size - filled), order.size - order.filled_size);
        order.last_trade_price = order.price;
        order.last_trade_id    = n_trades_++;
        order.filled_size     += order.last_trade_size;
        filled += order.last_trade_size;

        auto& instrument = core[order.inst_id];
        double first_leg_price = (instrument.type == InstrumentType::SPREAD()) ? core[instrument.sell_leg_id].get_ticker().last_trade_price : NAN;
        assert(!(instrument.type == InstrumentType::SPREAD() and std::isnan(first_leg_price)));

        if (instrument.type == InstrumentType::SPREAD())
        {
            {
                auto& order_recv = get_output<outputs::order_recv()>();
                order_recv.copy_from(order);
                order_recv.inst_id          = instrument.sell_leg_id;
                order_recv.side             = !order.side;
                order_recv.last_trade_price = first_leg_price;
                commit<outputs::order_recv()>();
            }

            {
                auto& order_recv = get_output<outputs::order_recv()>();
                order_recv.copy_from(order);
                order_recv.inst_id          = instrument.buy_leg_id;
                order_recv.side             = order.side;
                order_recv.last_trade_price = first_leg_price + order.price;
                commit<outputs::order_recv()>();
            }
        }
        else
        {
            auto& order_recv = get_output<outputs::order_recv()>();
            order_recv.copy_from(order);
            commit<outputs::order_recv()>();
        }

        if (order.filled_size == order.size)
        {
            order.event = Order::Event::CANCEL();
            auto& order_recv = get_output<outputs::order_recv()>();
            order_recv.copy_from(order);
            commit<outputs::order_recv()>();

            open_.erase(it->second);
            price_map.erase(it);
        }
    }
    return filled;
}

void Executer::activate(InputId input_channel_id)
{
    switch(input_channel_id.first)
    {
        case inputs::mktdata():
        {
            if (fill_model_ == FillModel::ALWAYS())
            {
                assert(open_.empty());
                break;
            }

            auto& mktdata = get_input<inputs::mktdata()>(input_channel_id);
            auto& event = mktdata.get_event();
            auto& book = core[event.inst_id].get_book();
            auto& ubook = core[event.inst_id].get_ubook();
            double tick_size = core[event.inst_id].tick_size;
	    
            string current_symbol = core[mktdata.inst_id].name;
            int current_md_exch = event.source;
            int current_md_stream = event.partition;

	    log_.writeln(std::forward_as_tuple(current_md_exch,current_md_stream,event.count,"mktdata",event.exchange_time.get_value(),event.exchange_time,0,0,0,0,0));

            if(feed_counter.find(current_md_exch) == feed_counter.end() ) feed_counter[current_md_exch] = std::unordered_map<int, int>();
            if(feed_counter.find(current_md_exch) != feed_counter.end() && feed_counter[current_md_exch].find(current_md_stream) ==  feed_counter[current_md_exch].end()  ) feed_counter[current_md_exch][current_md_stream] = 0;
            if(name_counter.find(mktdata.inst_id) == name_counter.end() ) name_counter[mktdata.inst_id] = 0;
            
            //uint64_t key = (uint64_t(current_md_exch) << 32 | uint64_t(current_md_stream));
            //if (feed_exchange_time.find(key) == feed_exchange_time.end())
            //    feed_exchange_time[key] = event.exchange_time;
            //
            //if (feed_md_count.find(key) == feed_md_count.end())
            //    feed_md_count[key] = event.count;
            //
            //auto threshold = queued_threshold_;
            //if (event.count > feed_md_count[key]+1)
            //{
            //    if (feed_order_id.find(key) == feed_order_id.end())
            //        feed_order_id[key] = event.as_add().order_id;
            //        
	    //    auto num_of_events_occured_at_exchange_since_last_event = event.count - feed_md_count[key];
	    //    // num_of_event_occured_at_exchange_since_last_event += event.as_add().order_id - feed_order_id[key]; 
	    //    
            //    //threshold = queued_threshold_ * (num_of_events_occured_at_exchange_since_last_event);
            //    threshold = queued_threshold_ * (num_of_events_occured_at_exchange_since_last_event + 1) * 0.5;

            //    feed_order_id[key] = std::max(event.as_add().order_id, feed_order_id[key]);
            //}
            //feed_md_count[key] = event.count;

            //feed_queued[key] =((event.exchange_time - feed_exchange_time[key]) < threshold);
            //feed_exchange_time[key] = event.exchange_time;
            //

            if(md_time.find(current_md_exch) == md_time.end()) md_time[current_md_exch] = std::unordered_map<int, grammar::datetime>();
            
            md_time[current_md_exch][current_md_stream] = event.exchange_time;
            
            feed_counter[current_md_exch][current_md_stream] = feed_counter[current_md_exch][current_md_stream] + 1;
            name_counter[mktdata.inst_id] = name_counter[mktdata.inst_id] + 1;

            if(id_exchange_map.find(mktdata.inst_id) == id_exchange_map.end() ) id_exchange_map[mktdata.inst_id] = current_md_exch;
            if(id_feed_map.find(mktdata.inst_id) == id_feed_map.end() ) id_feed_map[mktdata.inst_id] = current_md_stream;
            
            
            if(fill_model_ == FillModel::QP())
            {
                if(book.is_crossed() && !event.was_crossed())
                {
                    if(event.is_add())
                    {
                        auto& add = event.as_add();
                        crossmaker[mktdata.inst_id].oid = add.order_id;
                        crossmaker[mktdata.inst_id].side = add.side;
                        double limit_px = (add.side != Side::BUY()) ? std::max(add.px, book.best_bid() + tick_size) : std::min(add.px, book.best_ask() - tick_size);
                        assert(top_fill_size[event.inst_id] == 0);
                        top_fill_size[event.inst_id] += fill_limit_orders(event.inst_id, limit_px , add.qty, !add.side);
                    }
                    else if(event.is_modify())
                    {
                        auto& mod = event.as_modify();
                        assert( ((mod.side == Side::BUY()) && (mod.old_px < mod.new_px)) || ((mod.side == Side::SELL()) && (mod.old_px > mod.new_px)) );
                        crossmaker[mktdata.inst_id].oid = mod.old_order_id;
                        crossmaker[mktdata.inst_id].side = mod.side;
                        double limit_px = (mod.side != Side::BUY()) ? std::max(mod.new_px, book.best_bid() + tick_size) : std::min(mod.new_px , book.best_ask() - tick_size);
                        assert(top_fill_size[event.inst_id] == 0);
                        top_fill_size[event.inst_id] += fill_limit_orders(event.inst_id, limit_px , mod.new_qty, !mod.side);
                    }
                }
                else if (!book.is_crossed() && event.was_crossed())
                {
                    auto& order_map = mutable_core[mktdata.inst_id].get_order_map(crossmaker[mktdata.inst_id].side);
                    if (event.is_modify())
                    {
                        auto& modify = event.as_modify();
                        auto qty = modify.new_qty - ( ((modify.side == Side::BUY() and modify.new_px <= modify.old_px) or
                            (modify.side == Side::SELL() and modify.new_px >= modify.old_px)) ? modify.old_qty : 0);
                        fill_limit_orders(event.inst_id, modify.new_px, qty, !modify.side);
                        if(crossmaker[mktdata.inst_id].oid == modify.old_order_id)
                        {
                            auto px = modify.new_px;
                            auto qty = modify.new_qty;
                            fill_limit_orders(mktdata.inst_id, px, qty, !crossmaker[mktdata.inst_id].side);
                        }
                        else
                        {
                            auto it = (order_map).find(crossmaker[mktdata.inst_id].oid);
                            if(it != order_map.end())
                            {
                                // crossmaker order isn't exhausted
                                auto px = (it->second).px;
                                auto qty = (it->second).qty;
                                fill_limit_orders(mktdata.inst_id, px, qty, !crossmaker[mktdata.inst_id].side );
                            }
                        }
                        crossmaker[mktdata.inst_id].oid = 0;
                        top_fill_size[mktdata.inst_id] = 0;
                    }
                    else if (event.is_remove())
                    {
                        auto& remove = event.as_remove();
                        if(crossmaker[mktdata.inst_id].oid != remove.order_id)
                        {
                            auto it = (order_map).find(crossmaker[mktdata.inst_id].oid);
                            if(it != order_map.end())
                            {
                                // crossmaker order isn't exhausted
                                auto px = (it->second).px;
                                auto qty = (it->second).qty;
                                fill_limit_orders(mktdata.inst_id, px, qty, !crossmaker[mktdata.inst_id].side );
                            }
                        }
                        crossmaker[mktdata.inst_id].oid = 0;
                        top_fill_size[mktdata.inst_id] = 0;
                    }
                    else if (event.is_trade())
                    {
                        auto& trade = event.as_trade();
                        auto it = (order_map).find(crossmaker[mktdata.inst_id].oid);
                        if(it != order_map.end())
                        {
                            assert(crossmaker[mktdata.inst_id].uncrossed_by_trade == false);
                            crossmaker[mktdata.inst_id].uncrossed_by_trade = true;
                            crossmaker[mktdata.inst_id].uncrosser_trade_px = (it->second).px ;
                            crossmaker[mktdata.inst_id].remaining_qty = (it->second).qty - trade.qty;
                        }
                    }
                }
                else if (!book.is_crossed() && !event.was_crossed())
                {
                    if (event.is_add())
                    {
                        auto& add = event.as_add();
                        fill_limit_orders(event.inst_id, add.px, add.qty, !add.side);
                    }
                    else if (event.is_modify())
                    {
                        auto& modify = event.as_modify();
                        auto qty = modify.new_qty - ( ((modify.side == Side::BUY() and modify.new_px <= modify.old_px) or
                            (modify.side == Side::SELL() and modify.new_px >= modify.old_px)) ? modify.old_qty : 0);
                        fill_limit_orders(event.inst_id, modify.new_px, qty, !modify.side);
                    }
                }
                else
                {
                    //book.is_crossed() && book.was_crossed()

                    // Add events may also occur, close to post_trade sessions

                    if (event.is_modify())
                    {
                        auto& modify = event.as_modify();
                        auto qty = modify.new_qty - ( ((modify.side == Side::BUY() && modify.new_px <= modify.old_px) ||
                            (modify.side == Side::SELL() && modify.new_px >= modify.old_px)) ? modify.old_qty : 0);
                        double limit_px = (modify.side != Side::BUY()) ? std::max(modify.new_px, book.best_bid() + tick_size) : std::min(modify.new_px , book.best_ask() - tick_size);
                        top_fill_size[event.inst_id] += fill_limit_orders(event.inst_id, limit_px , modify.new_qty, !modify.side);
                        old_wqp[modify.side][event.inst_id] = modify.old_wqp;
                    }
                }
                if(event.is_trade())
                {
                    auto& trade = event.as_trade();
                    size_t fill_req_qty = trade.qty;
                    if(top_fill_size[event.inst_id] )
                    {
                        if(top_fill_size[event.inst_id] > trade.qty)
                        {
                            top_fill_size[event.inst_id] -= trade.qty;
                            fill_req_qty = 0;
                        }
                        else
                        {
                            fill_req_qty -= top_fill_size[event.inst_id];
                            top_fill_size[event.inst_id] = 0;
                        }
                    }
                    auto trade_wqp = (trade.side == Side::BUY()) ? trade.buy_wqp : trade.sell_wqp;
                    if(old_wqp[trade.side][mktdata.inst_id])
                    {
                        trade_wqp = old_wqp[trade.side][mktdata.inst_id];
                        old_wqp[trade.side][mktdata.inst_id] = 0;
                        old_wqp[!trade.side][mktdata.inst_id] = 0;
                    }
                    fill_early_orders(mktdata.inst_id, trade.px, fill_req_qty, trade.side, trade_wqp );
                    if(crossmaker[mktdata.inst_id].uncrossed_by_trade)
                    {
                        size_t fill_req_qty = crossmaker[mktdata.inst_id].remaining_qty;
                        if(top_fill_size[event.inst_id] )
                        {
                            if(top_fill_size[event.inst_id] >crossmaker[mktdata.inst_id].remaining_qty)
                            {
                                top_fill_size[event.inst_id] -= crossmaker[mktdata.inst_id].remaining_qty;
                                fill_req_qty = 0;
                            }
                            else
                            {
                                fill_req_qty -= top_fill_size[event.inst_id];
                                top_fill_size[event.inst_id] = 0;
                            }
                        }
                        fill_limit_orders(mktdata.inst_id, crossmaker[mktdata.inst_id].uncrosser_trade_px,
                                          fill_req_qty, !crossmaker[mktdata.inst_id].side );
                        crossmaker[mktdata.inst_id].uncrossed_by_trade = false;
                        crossmaker[mktdata.inst_id].oid = 0;
                    }
                }
                break;
            }
            if (event.is_touch_qty() && !book.is_crossed())
            {
                if(event.is_add())
                {
                    auto& add = event.as_add();
                    if(add.is_px_added())
                    {
                        touch_map_[add.side][event.inst_id].erase(book.px(1, add.side));
                    }
                }
                else if(event.is_remove())
                {
                    auto& cancel = event.as_remove();
                    if(cancel.is_px_removed())
                    {
                        touch_map_[cancel.side][event.inst_id].erase(cancel.px);
                    }
                    if(book.qty(0, cancel.side) <= touch_map_[cancel.side][event.inst_id][book.px(0, cancel.side)])
                    {
                        touch_map_[cancel.side][event.inst_id][book.px(0, cancel.side)] = book.qty(0, cancel.side);
                    }
                }
                else if(event.is_modify())
                {
                    auto& modify = event.as_modify();
                    if(modify.old_rank == 0)
                    {
                        if(modify.is_old_px_removed())
                        {
                            touch_map_[modify.side][event.inst_id].erase(modify.old_px);
                        }
                    }
                    if(modify.new_rank == 0)
                    {
                        if(modify.is_new_px_added())
                        {
                            touch_map_[modify.side][event.inst_id].erase(book.px(1, modify.side));
                        }
                    }
                    if(book.qty(0, modify.side) <= touch_map_[modify.side][event.inst_id][book.px(0, modify.side)])
                    {
                        touch_map_[modify.side][event.inst_id][book.px(0, modify.side)] = book.qty(0, modify.side);
                    }
                }
            }

            if (event.is_trade())
            {
                auto& trade = event.as_trade();
                auto side = trade.side;
                if(trade.side == Side::BUY())
                {
                    if(trade.is_buy_px_removed())
                    {
                        touch_map_[trade.side][event.inst_id].erase(trade.px);
                    }
                }
                else
                {
                    if(trade.is_sell_px_removed())
                    {
                        touch_map_[trade.side][event.inst_id].erase(trade.px);
                    }
                }
                if(book.qty(0, trade.side) <= touch_map_[trade.side][event.inst_id][book.px(0, trade.side)])
                {
                    touch_map_[trade.side][event.inst_id][book.px(0, trade.side)] = book.qty(0, trade.side);
                }
                if (fill_model_ == FillModel::LEVEL_FIRST() || fill_model_ == FillModel::LEVEL_FIRST_NOREPEAT())
                {
                    fill_limit_orders(event.inst_id, trade.px, trade.qty, trade.side);

                    //TODO : remove below line
		                //fill_limit_orders(event.inst_id, trade.px, trade.qty, !trade.side);
                }
                else if (fill_model_ == FillModel::LEVEL_LAST() || fill_model_ == FillModel::LEVEL_LAST_NOREPEAT())
                {
                    double tick_size = core[event.inst_id].tick_size;
                    fill_limit_orders(event.inst_id, trade.px + tick_size*( (trade.side == Side::BUY()) ? 1:-1 ), trade.qty, trade.side);
                }
            }

            if (book.best_bid() >= book.best_ask())
                break;

            if (event.is_add())
            {
                auto& add = event.as_add();
                fill_limit_orders(event.inst_id, add.px, add.qty, !add.side);
            }

            else if (event.is_modify())
            {
                auto& modify = event.as_modify();
                auto qty = modify.new_qty - ( ((modify.side == Side::BUY() and modify.new_px <= modify.old_px) or
                    (modify.side == Side::SELL() and modify.new_px >= modify.old_px)) ? modify.old_qty : 0);
                fill_limit_orders(event.inst_id, modify.new_px, qty, !modify.side);
            }

            break;
        }
        case inputs::order_send():
        {
            auto order_send = get_input<inputs::order_send()>(input_channel_id);

            if(order_map_name.find(order_send.order_id) == order_map_name.end() ) order_map_name[order_send.order_id] = name_counter[order_send.inst_id];
            if(order_map_feed.find(order_send.order_id) == order_map_feed.end() ) order_map_feed[order_send.order_id] = feed_counter[id_exchange_map[order_send.inst_id]][id_feed_map[order_send.inst_id]];

            mutable_core.notify_transmit();
            base::core.collect(false);
            if (exchange_ != Exchange::COMMON())
            {
                auto& instrument = core[order_send.inst_id];
                if (instrument.exchange != exchange_)
                    break;
            }

            if (delay_queue_.size() >= max_queue_size_)
            {
                mantle::enqueue_order_fail(order_send);
                break;
            }

            #if defined(HFT_MANTLE_ORDER_DUMMY)
            if (order_send.type == Order::Type::DUMMY())
            {
                mantle::enqueue_order_fail(order_send);
                break;
            }
            #endif
            auto tp = get_channel_timestamp(input_channel_id).combine(network_delay_);
    	    auto& exchange_last_time = md_time[id_exchange_map[order_send.inst_id]][id_feed_map[order_send.inst_id]];
	    order_send.deadline = exchange_last_time;
            base::core.event_queue.push(make_tuple(tp, this));
            delay_queue_.push(make_tuple(tp, order_send, delay_phase::NETWORK()));
            break;
        }
        case inputs::nsefo_exchtp():
        {
            auto& nsefo_exchtp = get_input<inputs::nsefo_exchtp()>(input_channel_id);

                  auto& current_md_exch=nsefo_exchtp[indicator::nsefo_exchtp::source()];
                  auto& current_md_stream=nsefo_exchtp[indicator::nsefo_exchtp::partition()];
                  auto& packet_count=nsefo_exchtp[indicator::nsefo_exchtp::seqno()];
                  auto& timestamp=nsefo_exchtp[indicator::nsefo_exchtp::timestamp()];
		  uint64_t packet_exchange_time=uint64_t(timestamp)+315513000000000000;

	          uint64_t key = (uint64_t(current_md_exch) << 32 | uint64_t(current_md_stream));
            	  if (feed_exchange_time.find(key) == feed_exchange_time.end())
            	      feed_exchange_time[key] = packet_exchange_time;
            	  
            	  if (feed_md_count.find(key) == feed_md_count.end())
            	      feed_md_count[key] = packet_count;
            	  
            	  auto threshold = queued_threshold_/grammar::timedelta::nanosecond();

            	  //if (packet_count > feed_md_count[key]+1)
            	  //{
            	  //    //if (feed_order_id.find(key) == feed_order_id.end())
            	  //    //    feed_order_id[key] = event.as_add().order_id;
            	  //        
	    	  //    auto num_of_events_occured_at_exchange_since_last_event = packet_count - feed_md_count[key];
	    	  //    // num_of_event_occured_at_exchange_since_last_event += event.as_add().order_id - feed_order_id[key]; 
	    	  //    
            	  //    //threshold = queued_threshold_ * (num_of_events_occured_at_exchange_since_last_event);
            	  //    threshold = queued_threshold_ * (num_of_events_occured_at_exchange_since_last_event + 1) * 0.5;

            	  //    //feed_order_id[key] = std::max(event.as_add().order_id, feed_order_id[key]);
            	  //}
            	  feed_md_count[key] = packet_count;

            	  feed_queued[key] =((packet_exchange_time - feed_exchange_time[key]) < threshold);
            	  feed_exchange_time[key] = packet_exchange_time;

                  if(md_time.find(current_md_exch) == md_time.end()) md_time[current_md_exch] = std::unordered_map<int, grammar::datetime>();
                  
                  md_time[current_md_exch][current_md_stream] = grammar::datetime(packet_exchange_time);

		  log_.writeln(std::forward_as_tuple(current_md_exch,current_md_stream,packet_count,"exchtp",packet_exchange_time,md_time[current_md_exch][current_md_stream],threshold,0,0,0,0,0));

	    break;
	   // cout << source << partition << count << exchtime << endl;
	
	}
    }
}
