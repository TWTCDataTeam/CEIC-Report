#################################################################################
# Enviroment :
##   Script   :   data_processing.py
##
# Author     :   Henry Hsieh
##   Date       :   2022-11-21
# Description:   The function of processing CEIC data
# Source     :
# Reference  :
# Target     :
# Outfile    :
# Requester  :
# Change History :
# --------------------------------------------------------------------------
# Date      Authors         Description/Ref. No.
# ----------   --------------- ---------------------------------------------
#################################################################################

import glob
import pandas as pd
from ceic_config import *
from datetime import datetime
#from main_parameter import CTY_MAP, TABLE2_DICT, TABLE3_PART1_DICT, TABLE3_PART2_DICT, TAIWAN_EXPORT_DICT, KOREA_EXPORT_DICT, JAPAN_EXPORT_DICT, CHINA_EXPORT_DICT, TABLE5_COUNTRY_ITEM_MAP, TABLE5_COL_ITEM

# TABLE 1-4

# find out the rawdata

# find out file path


def find_rawdata_path():
    file_list = glob.glob(RAWDATA_SOURCE+'/*.xlsx')
    return file_list

# loading file


def loading_tw_rawdata():
    # find file path
    file_list = find_rawdata_path()
    # loading file
    open_file = file_list[0]
    print("current_file:{}".format(open_file))
    df = pd.read_excel(open_file)
    df['Year'] = df['Select this link and click Refresh/Edit Download to update data and add or remove series'].dt.year
    df['Month'] = df['Select this link and click Refresh/Edit Download to update data and add or remove series'].dt.month

    return df

# function of calculate grow rate


def cal_growRate(target_value, base_value):
    if base_value != 0:
        growRate = round(((target_value/base_value)-1)*100, 4)
    else:
        growRate = "inf"
    return growRate

# function of calculate grow rate between trarge column and base column


def cal_column_grow_rate(df, target_column, base_column):
    col_growRate = []
    for i, j in zip(df[target_column].tolist(), df[base_column].tolist()):
        if pd.isna(i):
            col_growRate.append(i)
        else:
            month_gorwRate = cal_growRate(i, j)
            col_growRate.append(month_gorwRate)

    return col_growRate


# building table1
def table1_part1(df, setting_t_year):
    selected_year = setting_t_year
    selected_year_list = [str(selected_year-1), str(selected_year)]
    selected_columns = ['Year', 'Month', 'Total Export: Custom Clearance: USD']
    total_export = df.loc[df.Year.isin(selected_year_list), selected_columns]
    print(total_export)
    total_export = total_export.pivot(
        index='Month', columns='Year', values='Total Export: Custom Clearance: USD').reset_index()
    print(total_export)
    total_export = total_export.loc[:, ['Month', 2022, 2021]]
    total_export['ex_growRate'] = cal_column_grow_rate(
        total_export, 2022, 2021)

    return total_export

# building table2


def table1_part2(df, setting_t_year):
    selected_year = setting_t_year
    selected_year_list = [str(selected_year-1), str(selected_year)]
    selected_columns = ['Year', 'Month', 'Total Import: Custom Clearance: USD']
    total_import = df.loc[df.Year.isin(selected_year_list), selected_columns]
    total_import = total_import.pivot(
        index='Month', columns='Year', values='Total Import: Custom Clearance: USD').reset_index()
    total_import = total_import.loc[:, ['Month', 2022, 2021]]
    total_import['im_growRate'] = cal_column_grow_rate(
        total_import, 2022, 2021)

    return total_import

# building table3


def table1_part3(df, setting_t_year):
    selected_year = setting_t_year
    selected_year_list = [str(selected_year-1), str(selected_year)]
    selected_columns = ['Year', 'Month', 'Total Export: Custom Clearance: USD',
                        'Total Import: Custom Clearance: USD']
    trade_balance = df.loc[df.Year.isin(selected_year_list), selected_columns]
    trade_balance['trade_balance'] = trade_balance['Total Export: Custom Clearance: USD'] - \
        trade_balance['Total Import: Custom Clearance: USD']
    trade_balance = trade_balance.loc[:, ['Year', 'Month', 'trade_balance']]
    trade_balance = trade_balance.pivot(
        index='Month', columns='Year', values='trade_balance').reset_index()
    trade_balance = trade_balance.loc[:, ['Month', 2022, 2021]]

    return trade_balance

# sum column value


def sum_column_value(data, month_list, selected_column_name):
    data = data.loc[data['Month'].isin(month_list), [selected_column_name]]
    output = round(sum(data[selected_column_name]), 1)
    return output

# caculate total export value, to latest Month, in table1


def table1_sumToLatestMonth(df, setting_t_month):
    #  ??????1?????????????????????
    latest_month = setting_t_month
    latest_month_list = [(m+1) for m in range(0, latest_month)]
    ex_2022 = sum_column_value(df, latest_month_list, '2022_ex')
    ex_2021 = sum_column_value(df, latest_month_list, '2021_ex')
    acc_ex_growRate = cal_growRate(ex_2022, ex_2021)
    im_2022 = sum_column_value(df, latest_month_list, '2022_im')
    im_2021 = sum_column_value(df, latest_month_list, '2021_im')
    acc_im_growRate = cal_growRate(im_2022, im_2021)
    trade_bal_2022 = sum_column_value(df, latest_month_list, 2022)
    trade_bal_2021 = sum_column_value(df, latest_month_list, 2021)
    sumToLatestMonth = [str(latest_month_list[0])+"-"+str(latest_month_list[-1])+"?????????", ex_2022,
                        ex_2021, acc_ex_growRate, im_2022, im_2021, acc_im_growRate, trade_bal_2022, trade_bal_2021]

    return sumToLatestMonth

# caculate whole year value


def cal_whole_year_culumn(df):
    # ????????????
    whole_month = [(m+1) for m in range(0, 12)]
    #tmp = table1[table1.Year.isin(['2021'])]
    total_ex_2021 = sum_column_value(df, whole_month, '2021_ex')
    total_im_2021 = sum_column_value(df, whole_month, '2021_im')
    total_trade_balance_2021 = sum_column_value(df, whole_month, 2021)
    totalColumn = ['??????', "-", total_ex_2021, "-", "-",
                   total_im_2021, "-", "-", total_trade_balance_2021]

    return totalColumn

# change month name


def change_month_name(df):
    # ???Month?????????????????? X???
    month_list = []
    for e in df.Month.tolist():
        e = str(e)
        if e[-1] == "???":
            break
        else:
            month_string = str(e)+"???"
            month_list.append(month_string)

    df['Month'] = month_list

    return df

# organization whole inforamtio in table 1


def table1_merge_and_processing(df, setting_t_year, setting_t_month):

    total_export = table1_part1(df, setting_t_year)
    total_import = table1_part2(df, setting_t_year)
    trade_balance = table1_part3(df, setting_t_year)

    table1 = total_export.merge(
        total_import, on='Month', how='left', suffixes=('_ex', '_im'))
    table1 = table1.merge(trade_balance, on='Month',
                          how='left').reset_index(drop=True)

    #  ??????1?????????????????????
    sumToLatestMonth_list = table1_sumToLatestMonth(table1, setting_t_month)
    # ??????2021???????????????
    total_column_list = cal_whole_year_culumn(table1)

    # ???Month?????????????????? X???
    table1 = change_month_name(table1)

    # ??????sumToLatestMonth_list ??? total_column_list
    summary_df = pd.DataFrame([sumToLatestMonth_list, total_column_list], columns=['Month',     '2022_ex',     '2021_ex', 'ex_growRate',
                                                                                   '2022_im',     '2021_im', 'im_growRate',          2022,
                                                                                   2021])

    # ???table1????????? summary_df
    output_table1 = pd.concat([table1, summary_df],
                              axis=0).reset_index(drop=True)
    output_table1 = output_table1.fillna("-")

    # change table1 column name
    new_column = {'Month': '??????',
                  '2022_ex': str(setting_t_year)+'??????',
                  '2021_ex': str((setting_t_year-1))+'??????',
                  'ex_growRate': '???????????????',
                  '2022_im': str(setting_t_year)+'??????',
                  '2021_im': str((setting_t_year-1))+'??????',
                  'im_growRate': '???????????????',
                  2022: str(setting_t_year)+'?????????',
                  2021: str((setting_t_year-1))+'?????????'}
    output_table1 = output_table1.rename(columns=new_column)

    return output_table1

# ?????????????????????????????????


def areaSum_monthYearFilter(df, target_year, target_month, attr_list):
    year_list = [target_year]
    month_list = [target_month]
    areaSum = 0.0
    for attr_item in attr_list:
        cty_value = df.loc[(df.Year.isin(year_list)) & (
            df.Month.isin(month_list)), [attr_item]][attr_item].sum()
        areaSum = areaSum + cty_value

    return areaSum


# ?????????????????????xx???1?????????????????????
def sumToLatestMonth_Filter(df, target_year, target_month, attr_list):
    year_list = [target_year]
    month_list = [(i+1) for i in range(0, target_month)]
    areaSum = 0.0
    for attr_item in attr_list:
        cty_value = df.loc[(df.Year.isin(year_list)) & (
            df.Month.isin(month_list)), [attr_item]][attr_item].sum()
        areaSum = areaSum + cty_value

    return areaSum

# sum totale value in setting year and setting area


def sumYear_Filter(df, target_year, attr_list):
    year_list = [target_year]
    areaSum = 0.0
    for attr_item in attr_list:
        cty_value = df.loc[(df.Year.isin(year_list)), [
            attr_item]][attr_item].sum()
        areaSum = areaSum + cty_value

    return areaSum


# ????????????????????????
def latest_month_areaSum(df, cty_dict, target_year, target_month, area_name, area_list):
    if area_list[0] != "blank":
        # ?????????????????????????????????????????????
        attr_list = []
        for area_cty_name in area_list:
            cty_attr = cty_dict[area_cty_name]
            attr_list.append(cty_attr)

        # ??????????????????
        this_year = target_year
        last_year = target_year-1
        this_year_value = areaSum_monthYearFilter(
            df, this_year, target_month, attr_list)
        last_year_value = areaSum_monthYearFilter(
            df, last_year, target_month, attr_list)
        growRate = cal_growRate(this_year_value, last_year_value)

        #print("area:{} this_yr:{} last_yr:{} grow_rate:{}".format(area_name,this_year_value, last_year_value, growRate))

        return [area_name, this_year_value, growRate]

    else:
        return [area_name, "-", "-"]


# ????????????xx???1??????x?????????
def sumToLatestMonth_areaSum(df, cty_dict, target_year, target_month, area_name, area_list):
    if area_list[0] != "blank":
        # ?????????????????????????????????????????????
        attr_list = []
        for area_cty_name in area_list:
            cty_attr = cty_dict[area_cty_name]
            attr_list.append(cty_attr)

        # ??????????????????
        this_year = target_year
        last_year = target_year-1
        this_year_value = sumToLatestMonth_Filter(
            df, this_year, target_month, attr_list)
        last_year_value = sumToLatestMonth_Filter(
            df, last_year, target_month, attr_list)
        growRate = cal_growRate(this_year_value, last_year_value)

        print("area:{} this_yr:{} last_yr:{} grow_rate:{}".format(
            area_name, this_year_value, last_year_value, growRate))

        return [area_name, this_year_value, growRate]
    else:
        return [area_name, "-", "-"]


# ????????????xx???????????????
def sumYear_areaSum(df, cty_dict, target_year, area_name, area_list):
    if area_list[0] != "blank":
        # ?????????????????????????????????????????????
        attr_list = []
        for area_cty_name in area_list:
            cty_attr = cty_dict[area_cty_name]
            attr_list.append(cty_attr)

        # ??????????????????
        this_year = target_year
        last_year = target_year-1
        this_year_value = sumYear_Filter(df, this_year, attr_list)
        last_year_value = sumYear_Filter(df, last_year, attr_list)
        growRate = cal_growRate(this_year_value, last_year_value)

        #print("area:{} this_yr:{} last_yr:{} grow_rate:{}".format(area_name,this_year_value, last_year_value, growRate))

        return [area_name, this_year_value, growRate]
    else:
        return [area_name, "-", "-"]

# caculate ratio


def caculate_ratio(df):
    total_sum = df.iloc[0, 1]
    col_ratio = []

    for value in df.iloc[:, 1].tolist():
        if value != "-":
            element_ratio = round((value/total_sum)*100, 4)
            col_ratio.append(element_ratio)
        else:
            col_ratio.append("-")

    new_col_name = df.columns[1]+"?????????"
    # ???????????????????????????
    df[new_col_name] = col_ratio

    return df

# processing table23 part1


def table23_part1(df, cty_map, table_dict, target_year, target_month):
    tmp_summary = []
    for cty_item, cty_list in table_dict.items():
        report_row = latest_month_areaSum(
            df, cty_map, target_year, target_month, cty_item, cty_list)
        tmp_summary.append(report_row)

    # transform to dataframe
    col_name = ['??????', "{}???{}?????????".format(
        target_year, target_month), "{}???{}????????????".format(target_year, target_month)]
    tmp_summary_df = pd.DataFrame(tmp_summary, columns=col_name)

    # create ratio col
    tmp_summary_df = caculate_ratio(tmp_summary_df)

    return tmp_summary_df

# processing table23 part2


def table23_part2(df, cty_map, table_dict, target_year, target_month):
    tmp_summary = []
    for cty_item, cty_list in table_dict.items():
        report_row = sumToLatestMonth_areaSum(
            df, cty_map, target_year, target_month, cty_item, cty_list)
        tmp_summary.append(report_row)

    # transform to dataframe
    col_name = ['??????', "{}???1-{}?????????".format(target_year, target_month),
                "{}???1-{}????????????".format(target_year, target_month)]
    tmp_summary_df = pd.DataFrame(tmp_summary, columns=col_name)

    # create ratio col
    tmp_summary_df = caculate_ratio(tmp_summary_df)

    return tmp_summary_df

# processing table23 part3


def table23_part3(df, cty_map, table_dict, t_year):
    tmp_summary = []
    for cty_item, cty_list in table_dict.items():
        report_row = sumYear_areaSum(df, cty_map, t_year, cty_item, cty_list)
        tmp_summary.append(report_row)

    # transform to dataframe
    col_name = ['??????', "{}?????????".format(t_year), "{}????????????".format(t_year)]
    tmp_summary_df = pd.DataFrame(tmp_summary, columns=col_name)

    # create ratio col
    tmp_summary_df = caculate_ratio(tmp_summary_df)

    return tmp_summary_df

# combine part1, part2, and part3


def table_merge_output(df, cty_map, dict_name, t_year, t_month):

    print("==== start ====")
    part1 = table23_part1(df, cty_map, dict_name, t_year, t_month)
    part2 = table23_part2(df, cty_map, dict_name, t_year, t_month)
    last_yr = t_year-1
    part3 = table23_part3(df, cty_map, dict_name, last_yr)

    print("==== merge ====")
    output = part1.merge(part2, on=['??????'], how='left')
    output = output.merge(part3, on=['??????'], how='left')

    return output


# TABLE5
# loading ???????????????????????????????????????????????????(new)_v3 file
def loading_main_competer_data():

    file_list = find_rawdata_path()
    # loading raw data ???????????????????????????????????????????????????(new)_v3
    open_file = file_list[1]
    print("current_file:{}".format(open_file))
    df2 = pd.read_excel(open_file)
    df2['Year'] = df2['Select this link and click Refresh/Edit Download to update data and add or remove series'].dt.year
    df2['Month'] = df2['Select this link and click Refresh/Edit Download to update data and add or remove series'].dt.month

    return df2

# processing data of taiwan


def table_taiwan(dataframe, target_cty, cty_dict, target_year, target_month, col_dict):
    output_list = []
    for col_name, col_list in col_dict.items():
        if col_name == '??????':
            output_list.append(['??????', "-", "-"])
        elif col_name == '2021???????????????????????????':
            output_list.append(['2021???????????????????????????', "-", "-"])
        else:
            tmp = sumToLatestMonth_areaSum(
                dataframe, cty_dict, target_year, target_month, col_name, col_list)
            output_list.append(tmp)

    target_colname = ['??????', target_cty+'??????', target_cty+'?????????']
    output_df = pd.DataFrame(output_list, columns=target_colname)

    output_df = caculate_ratio(output_df)

    output_df = output_df.iloc[:, [0, 2, 3]]

    return output_df

# processing data of korea


def table_korea(dataframe, target_cty, cty_dict, target_year, target_month, col_dict):
    output_list = []
    for col_name, col_list in col_dict.items():
        if col_name == '??????':
            output_list.append(['??????', "-", "-"])
        elif col_name == '2021???????????????????????????':
            output_list.append(['2021???????????????????????????', "-", "-"])
        else:
            tmp = sumToLatestMonth_areaSum(
                dataframe, cty_dict, target_year, target_month, col_name, col_list)
            output_list.append(tmp)

    target_colname = ['??????', target_cty+'??????', target_cty+'?????????']
    output_df = pd.DataFrame(output_list, columns=target_colname)

    output_df = caculate_ratio(output_df)

    output_df = output_df.iloc[:, [0, 2, 3]]

    return output_df

# processing data of japan


def table_japan(dataframe, target_cty, cty_dict, target_year, target_month, col_dict):
    output_list = []
    for col_name, col_list in col_dict.items():
        if col_name == '??????':
            output_list.append(['??????', "-", "-"])
        elif col_name == '2021???????????????????????????':
            output_list.append(['2021???????????????????????????', "-", "-"])
        else:
            tmp = sumToLatestMonth_areaSum(
                dataframe, cty_dict, target_year, target_month, col_name, col_list)
            output_list.append(tmp)

    target_colname = ['??????', target_cty+'??????', target_cty+'?????????']
    output_df = pd.DataFrame(output_list, columns=target_colname)

    output_df = caculate_ratio(output_df)

    output_df = output_df.iloc[:, [0, 2, 3]]

    return output_df

# processing data of china


def table_china(dataframe, target_cty, cty_dict, target_year, target_month, col_dict):
    output_list = []
    for col_name, col_list in col_dict.items():
        if col_name == '?????????????????????':
            output_list.append(['?????????????????????', "-", "-"])
        elif col_name == '2021???????????????????????????':
            output_list.append(['2021???????????????????????????', "-", "-"])
        else:
            tmp = sumToLatestMonth_areaSum(
                dataframe, cty_dict, target_year, target_month, col_name, col_list)
            output_list.append(tmp)

    target_colname = ['??????', target_cty+'??????', target_cty+'?????????']
    output_df = pd.DataFrame(output_list, columns=target_colname)

    output_df = caculate_ratio(output_df)

    output_df = output_df.iloc[:, [0, 2, 3]]

    return output_df

# ????????????????????????


def get_date():
    time = '%0.2d%0.2d%0.2d%0.2d%0.2d' % (datetime.now().year, datetime.now(
    ).month, datetime.now().day, datetime.now().hour, datetime.now().minute)
    return time


def outputToExcel(df, file_name, sh_name, unit_name, table_header):
    writer = pd.ExcelWriter(file_name)
    df.to_excel(writer, sheet_name=sh_name, startrow=2, index=False)
    worksheet = writer.sheets[sh_name]
    worksheet.write_string(1, 0, unit_name)
    worksheet.write_string(0, 0, table_header)
    raw_num = df.shape[0]+3
    worksheet.write_string(raw_num, 0, "??????????????????????????????????????????????????????CEIC?????????")

    writer.save()
    print("{} output_complete".format(file_name))
