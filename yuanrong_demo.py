#! /usr/local/bin/python3

import time
import sys
import yuanrong
from turing_models.utilities.turing_date import TuringDate
from turing_models.utilities.global_types import TuringOptionTypes
from turing_models.utilities.day_count import TuringDayCountTypes
from turing_models.products.equity.equity_vanilla_option import TuringEquityVanillaOption
from turing_models.products.equity.equity_american_option import TuringEquityAmericanOption
from turing_models.models.model_black_scholes import TuringModelBlackScholes, TuringModelBlackScholesTypes
from turing_models.market.curves.discount_curve_flat import TuringDiscountCurveFlat

yuanrong.init(package_ref='sn:cn:yrk:12345678901234561234567890123456:function:0-computeprice-demo:$latest', logging_level='INFO', cluster_server_addr='124.70.194.241')

# --------------------------------------------------------------------------
# Section 1: Pricing
# --------------------------------------------------------------------------
# Params
v_value_date = TuringDate(y=2021, m=4, d=25)
v_expiry_date = TuringDate(y=2021, m=10, d=25)
v_strike_price = 500
v_volatility = 0.02
v_num_options = 100

v_stock_price = 510
v_interest_rate = 0.03
v_dividend_yield = 0


# Model Definition
v_model_tree = TuringModelBlackScholes(
    v_volatility,
    implementationType=TuringModelBlackScholesTypes.CRR_TREE,
    numStepsPerYear=10000)
v_discount_curve = TuringDiscountCurveFlat(
    v_value_date,
    v_interest_rate,
    dayCountType=TuringDayCountTypes.ACT_365F)
v_dividend_curve = TuringDiscountCurveFlat(
    v_value_date,
    v_dividend_yield,
    dayCountType=TuringDayCountTypes.ACT_365F)


# 可以修改循环次数
@yuanrong.ship()
def compute_price(value_date, expiry_date, strike_price, stock_price, discount_curve, dividend_curve, model_tree, num_options):
    # Option Definition
    start = time.time()
    vanilla_option = TuringEquityVanillaOption(
        expiry_date, strike_price, TuringOptionTypes.EUROPEAN_CALL)

    american_option = TuringEquityAmericanOption(
        expiry_date, strike_price, TuringOptionTypes.AMERICAN_PUT)

    # Pricing
    vanilla_option_price_crr = vanilla_option.value_crr(
        value_date,
        stock_price,
        discount_curve,
        dividend_curve,
        model_tree) * num_options
    vanilla_option_price_mc = vanilla_option.valueMC_NUMPY_NUMBA(
        value_date,
        stock_price,
        discount_curve,
        dividend_curve,
        model_tree) * num_options
    vanilla_option_delta = vanilla_option.delta(
        value_date,
        stock_price,
        discount_curve,
        dividend_curve,
        model_tree) * num_options
    vanilla_option_gamma = vanilla_option.gamma(
        value_date,
        stock_price,
        discount_curve,
        dividend_curve,
        model_tree) * num_options
    vanilla_option_vega = vanilla_option.vega(
        value_date,
        stock_price,
        discount_curve,
        dividend_curve,
        model_tree) * num_options

    american_option_price = american_option.value(
        value_date,
        stock_price,
        discount_curve,
        dividend_curve,
        model_tree) * num_options

    ret = "[Vanilla] Option Price(use crr): {}, Option Price(use MC): {}, Delta: {}, Gamma: {}, Vega: {} [American] Option Price: {}".format(
        vanilla_option_price_crr, vanilla_option_price_mc,
        vanilla_option_delta, vanilla_option_gamma,
        vanilla_option_vega, american_option_price)
    end = time.time()
    import platform
    return (start, round((end-start),2)), platform.node()

loop = 5000
if len(sys.argv) == 1:
    print("ERROR: Please give me a concurrency num!!")
    sys.exit()
else:
    loop = int(sys.argv[1])

print("CONCURRENCY: %s" % loop)


time_start = time.time()
date_start = time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime())
print("BEGIN TO SHIP: %s (%s)\n" % (time_start, date_start))
idlist = [compute_price.ship(v_value_date, v_expiry_date, v_strike_price, v_stock_price, v_discount_curve, v_dividend_curve, v_model_tree, v_num_options) for i in range(loop)]
time_1 = time.time()
print("SHIP END: %s\n" % time_1)
print("BEGIN TO GET----\n")
retlist = yuanrong.get(idlist)
time_end = time.time()
date_end = time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime())
print("GET END: %s (%s)\n" % (time_end, date_end))

print("Time cost: %fs\n" % (time_end - time_start))

retinfo = {}
reqinfo = {}
for ret in retlist:
    if ret[1] in retinfo.keys():
        retinfo[ret[1]].append(ret[0])
        reqinfo[ret[1]] += 1
    else:
        retinfo[ret[1]] = []
        retinfo[ret[1]].append(ret[0])
        reqinfo[ret[1]] = 0
        reqinfo[ret[1]] += 1


filename = "detail.txt"
fd = open(filename, 'w')

for item in retinfo:
    fd.write("\n%s:%s %s\n" % (item, retinfo[item], reqinfo[item]))

fd.close()

print('Scale %s function instances\n' % len(retinfo))
for item in sorted(reqinfo.items(), key=lambda kv:(kv[1], kv[0]), reverse=True):
   print(item) 
