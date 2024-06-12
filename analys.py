from langchain.chains import LLMChain
from datetime import datetime, timedelta
from langchain_openai import ChatOpenAI
from app.engine.postgresql import db
from conf.one_api_key import get_model_token
from conf.settings import OPENAI_LIMIT_UP_MODEL
from langchain.prompts import HumanMessagePromptTemplate, ChatPromptTemplate
import time
from app.common.log import logger
import traceback

"""
Change Log:

[2024-1-16] - [魏谢文]
- 修改info search 接口检索逻辑 stock -> 6位码

"""


def to_check_v2_text_statu(query, sql, data):
    today = datetime.now().strftime("%Y-%m-%d")
    check_sql = "select * from spider_status where date = '{}' and st_code ='{}' limit 1" \
        .format(today, query['st_code'])
    for item in db.select_all_dict(check_sql):
        # 23.9.15 add v2text 状态
        if item['v2_text'] == '排队等待处理中' or item['v2_text'] == "暂无数据":
            logger.info('[涨停分析] v2状态未更新，再次更新')
            db.insertone(sql, data)
        else:
            logger.info('[涨停分析] v2数据已更新')


def to_update_v2_text_statu(dic):
    logger.info(f"[涨停分析] updating v2  , {dic['st_name']}")
    statuskey = ['query_index', 'st_code', 'st_name', 'up_limit_time', 'up_limit_statu',
                 'nb_statu', 'xgb_statu', 'ehd_statu', 'jygs_statu', 'hdy_statu',
                 'processing_flag', 'v1_text', 'v2_text', 'analy_first_finish_flag', 'analy_second_finish_flag',
                 'date', 'onboard_time', 'all_status']
    first_input_data = []
    for key in statuskey:
        first_input_data.append(dic[key])
    if len(first_input_data) == 18:
        sql_key_index = ['query_index', 'st_code', 'st_name', 'up_limit_time', 'up_limit_statu',
                         'nb_statu', 'xgb_statu', 'ehd_statu', 'jygs_statu', 'hdy_statu',
                         'processing_flag', 'v1_text', 'v2_text', 'analy_first_finish_flag', 'analy_second_finish_flag',
                         'date', 'onboard_time', 'all_status']
        sql_key_without_index = ['up_limit_time', 'up_limit_statu', 'nb_statu', 'xgb_statu', 'ehd_statu', 'jygs_statu',
                                 'hdy_statu',
                                 'processing_flag', 'v1_text', 'v2_text', 'analy_first_finish_flag',
                                 'analy_second_finish_flag',
                                 'onboard_time', 'all_status']
        sql_key_str = ','.join(sql_key_index)
        fields_values = ','.join(
            f'%s' for i in range(1, len(sql_key_index) + 1))
        sql_insert = f"""insert into spider_status({sql_key_str}) values({fields_values}) 
            on CONFLICT (st_code, date) do update
            SET {", ".join([f'{column} = EXCLUDED.{column}' for column in sql_key_without_index])}
            WHERE spider_status.st_code = EXCLUDED.st_code AND spider_status.date = EXCLUDED.date
            """
        db.insertone(sql_insert, first_input_data)
        return sql_insert, first_input_data
    else:
        raise Exception('data loss')


def to_analyse_v2_text(data_list, model_name, api_key):
    st_name = data_list[0]
    st_code = data_list[1]
    logger.info(f"[涨停分析] analying v2  , {st_name}")
    industry = ""
    for data in data_list[2]:
        for key in data.keys():
            industry += data[key]
            industry += '\n'
    # industry = ''.join([data[key] for data in data_list[2] for key in data.keys()])
    xgb_describtion = ""
    for data in data_list[3]:
        for key in data.keys():
            xgb_describtion += data[key]
            xgb_describtion += '\n'
    v1_text = ""
    for data in data_list[4]:
        for key in data.keys():
            v1_text += data[key]
            v1_text += '\n'
    jygs_text = ""
    for data in data_list[5]:
        for key in data.keys():
            jygs_text += data[key]
            jygs_text += '\n'
    xgb_24H_data = ""
    for data in data_list[6]:
        for key in data.keys():
            if data[key] == "":
                pass
            else:
                xgb_24H_data += data[key] + ' '
        xgb_24H_data += '\n'
    ehd_data = ""
    for data in data_list[7]:
        for key in data.keys():
            if key == 'msq_q':
                ehd_data += 'Q:'
                ehd_data += data[key]
                ehd_data += '\n'
            if key == 'msq_a':
                ehd_data += 'A:'
                ehd_data += data[key]
                ehd_data += '\n'
    hde_data = ""
    for data in data_list[8]:
        for key in data.keys():
            if key == 'msg_q':
                hde_data += 'Q:'
                hde_data += data[key]
                hde_data += '\n'
            if key == 'msg_a':
                hde_data += 'A:'
                hde_data += data[key]
                hde_data += '\n'
    # v2 delete
    co_basic_data = ""
    for data in data_list[9]:
        for key in data.keys():
            co_basic_data += data[key]
            co_basic_data += '\n'
    datadata_list = [st_name, st_code, industry, xgb_describtion,
                     v1_text, jygs_text, xgb_24H_data]  # ehd_data,hde_data
    text = '''
{} {}
归属行业: {}
结论1:
{}
结论2:
{}
结论3:
{}
结论4:
{}
'''.format(*datadata_list)
    # #####
    # {}
    # #####
    # {}
    text = text[:3000]
    logger.info(f"[涨停分析] {text}")
    content = limit_up_analyse_v2(api_key, model_name, text)
    today = datetime.now().strftime("%Y-%m-%d")
    inser_save_text_sql = """insert into sql_analyse_text_v1v2 (st_code, v1_input_text, v2_input_text, date , v1_text,v2_text) 
    values (%s,'',%s,%s,'',%s)"""
    db.insertone(inser_save_text_sql, [datadata_list[1], text, today, content])
    return content


def to_check_v1_text_statu(query, sql, data):
    today = datetime.now().strftime("%Y-%m-%d")
    check_sql = "select * from spider_status where date = '{}' and st_code like '%{}%' limit 1".format(today,
                                                                                                       query['st_code'])
    for item in db.select_all_dict(check_sql):
        if item['v1_text'] == '排队等待处理中':
            logger.info(f"[涨停分析] v1状态未更新，再次更新")
            db.insertone(sql, data)
        else:
            logger.info(f"[涨停分析] v1数据已更新")


def to_update_v1_text_statu(dic):
    logger.info(f"[涨停分析] updating v1  {dic['st_name']}")
    statuskey = ['query_index', 'st_code', 'st_name', 'up_limit_time', 'up_limit_statu',
                 'nb_statu', 'xgb_statu', 'ehd_statu', 'jygs_statu', 'hdy_statu',
                 'processing_flag', 'v1_text', 'v2_text', 'analy_first_finish_flag', 'analy_second_finish_flag',
                 'date', 'onboard_time', 'all_status']
    first_input_data = []
    for key in statuskey:
        first_input_data.append(dic[key])
    if len(first_input_data) == 18:
        sql_key_index = ['query_index', 'st_code', 'st_name', 'up_limit_time', 'up_limit_statu',
                         'nb_statu', 'xgb_statu', 'ehd_statu', 'jygs_statu', 'hdy_statu',
                         'processing_flag', 'v1_text', 'v2_text', 'analy_first_finish_flag', 'analy_second_finish_flag',
                         'date', 'onboard_time', 'all_status']
        sql_key_without_index = ['up_limit_time', 'up_limit_statu', 'nb_statu', 'xgb_statu', 'ehd_statu', 'jygs_statu',
                                 'hdy_statu',  # None spyder? hdy_check
                                 'processing_flag', 'v1_text', 'v2_text', 'analy_first_finish_flag',
                                 'analy_second_finish_flag',
                                 'onboard_time', 'all_status']
        sql_key_str = ','.join(sql_key_index)
        fields_values = ','.join(
            f'%s' for i in range(1, len(sql_key_index) + 1))
        sql_insert = f"""insert into spider_status({sql_key_str}) values({fields_values}) 
            on CONFLICT (st_code, date) do update
            SET {", ".join([f'{column} = EXCLUDED.{column}' for column in sql_key_without_index])}
            WHERE spider_status.st_code = EXCLUDED.st_code AND spider_status.date = EXCLUDED.date
            """
        logger.info(f"[涨停分析] 数据库写入  {first_input_data}  || {sql_insert}")
        db.insertone(sql_insert, first_input_data)
        return sql_insert, first_input_data
    else:
        raise Exception('data loss')


def to_collect_v2_data(result):
    # to collect basic info
    today = datetime.now().strftime("%Y-%m-%d")
    seven_days_ago = (datetime.strptime(today, "%Y-%m-%d") -
                      timedelta(days=7)).strftime("%Y-%m-%d")
    tirty_days_ago = (datetime.strptime(today, "%Y-%m-%d") -
                      timedelta(days=30)).strftime("%Y-%m-%d")
    # three_month = (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=90)).strftime("%Y-%m-%d")
    st_name = result['st_name']
    st_code = result['st_code']
    logger.info(f"[涨停分析] collecting v2   {st_name} ")
    industry_sql = "select industrycsrc1 from spider_company_info where security_code = '{}'".format(
        st_code)
    industry = db.select_all_dict(industry_sql)
    xgb_sql = "select distinct description from his_limit_up where code like '%{}%' and enter_time >'{}';".format(
        st_code, today)
    xgb_describtion = db.select_all_dict(xgb_sql)
    v1_text_sql = "select v1_text from spider_status where st_code = '{}' and date ='{}';".format(result['st_code'],
                                                                                                  today)
    v1_text = db.select_all_dict(v1_text_sql)
    jygs_sql = "select article_title, expound from spiders_jiuyangongshe where stock_code like '%{}%' and article_create_time > '{}';".format(
        st_code, today)
    jygs_text = db.select_all_dict(jygs_sql)
    # 1040 未采集
    # if result['xgb_statu'] == '1040':
    #     xgb_24H_data = [{}]
    # else:
    xgb_24H_sql = """
    SELECT to_char(timestamp,'YYYY-MM-DD HH24:MI:SS') as timestamp,title,summary FROM spider_instent_msgs, jsonb_array_elements(stocks) AS elements 
    WHERE substring(elements ->> 'symbol', 1, 6) = '{}' AND spider_instent_msgs."timestamp" > '{}';
    """.format(st_code, seven_days_ago)
    xgb_24H_data = db.select_all_dict(xgb_24H_sql)
    # if result['ehd_statu']=='1040':
    #     ehd_data = [{}]
    # else:
    ehd_sql = "select msg_q ,msg_a from spiders_ehudong where query_date > '{}' and stock_code like '{}' and msg_a  <> '' limit 5;".format(
        tirty_days_ago, st_code)
    ehd_data = db.select_all_dict(ehd_sql)
    hde_sql = "select msg_q ,msg_a from spiders_hudonge where query_date > '{}' and stock_code like '{}' and msg_a is not null limit 5;".format(
        tirty_days_ago, st_code)
    hde_data = db.select_all_dict(hde_sql)
    co_basic_sql = "select org_profile from spider_company_info where security_code = '{}'".format(
        st_code)
    co_basic_data = db.select_all_dict(co_basic_sql)
    return [st_name, st_code, industry, xgb_describtion, v1_text, jygs_text, xgb_24H_data, ehd_data, hde_data,
            co_basic_data]


def to_collect_v1_data(result):
    """收集v1分析数据信息

    Args:
        result ( key : object ): 单条要进行分析的股票数据信息

    Returns:
        [],{}: [st_name, st_code, industry, xgb_describtion, xgb_24H_data, ehd_data, hde_data, co_basic_data], reback_dic
    """    """"""
    today = datetime.now().strftime("%Y-%m-%d")
    tirty_days_ago = (datetime.strptime(today, "%Y-%m-%d") -
                      timedelta(days=30)).strftime("%Y-%m-%d")
    seven_days_ago = (datetime.strptime(today, "%Y-%m-%d") -
                      timedelta(days=7)).strftime("%Y-%m-%d")
    st_name = result['st_name']
    st_code = result['st_code']
    limit_up_time = str(result['up_limit_time'])
    logger.info(f"[涨停分析] collecting v1   {st_name} ")
    industry_sql = "select em2016 as industrycsrc1 from spider_company_info where security_code = '{}'".format(
        st_code)
    industry = db.select_all_dict(industry_sql)
    xgb_sql = "select distinct description from his_limit_up where code like '%{}%' and enter_time >'{}';".format(
        st_code, today)
    xgb_describtion = db.select_all_dict(xgb_sql)
    # if result['xgb_statu'] == '未采集':
    #     xgb_24H_data = [{}]
    # else:
    # 待确认这边sql是否变动
    xgb_24H_sql = """
    SELECT to_char(timestamp,'YYYY-MM-DD HH24:MI:SS') as timestamp,title,summary FROM spider_instent_msgs, jsonb_array_elements(stocks) AS elements 
    WHERE substring(elements ->> 'symbol', 1, 6) = '{}' AND spider_instent_msgs."timestamp" > '{}';
    """.format(st_code, seven_days_ago)
    xgb_24H_data = db.select_all_dict(xgb_24H_sql)
    ehd_sql = "select distinct mes_time,msg_q ,msg_a from spiders_ehudong where query_date > '{}' and stock_code like '%{}%' and msg_a  <> '' order by mes_time desc limit 5;".format(
        tirty_days_ago, st_code)
    ehd_data = db.select_all_dict(ehd_sql)
    hde_sql = "select distinct mes_time,msg_q ,msg_a from spiders_hudonge where query_date > '{}' and stock_code like '%{}%' and msg_a is not null order by mes_time desc limit 5;".format(
        tirty_days_ago, st_code)
    hde_data = db.select_all_dict(hde_sql)
    co_basic_sql = "select org_profile from spider_company_info where security_code = '{}'".format(
        st_code)
    co_basic_data = db.select_all_dict(co_basic_sql)
    jygs_his_sql = "select * from spiders_jiuyangongshe where stock_code like '%{}%' and article_create_time < '{}' order by article_create_time desc limit 1".format(
        st_code, today)
    jygs_his_data = db.select_all_dict(jygs_his_sql)
    top_gain_ban_sql = "select plates from his_limit_up where code like '%{}%' order by enter_time desc limit 1".format(
        st_code)
    top_gain_ban_result = db.select_all_dict(top_gain_ban_sql)
    if len(top_gain_ban_result) > 0:
        for data in top_gain_ban_result:
            top_gain_id = data['plates'][0]['id']
            bankuai_name = data['plates'][0]['name']
        gain_describtion_sql = "select  COALESCE(description, '') AS description from spiders_xuangubao_surge_stock where data_id = {} order by query_date desc limit 1".format(
            top_gain_id)
        gain_describtion_result = db.select_all_dict(gain_describtion_sql)
        for des_data in gain_describtion_result:
            # 板块涨停理由
            gain_describtion = des_data['description']
    # 暂无出现hislimitup上无该块数据的情况
    # else:
    #     top_gain_id = 0
    #     bankuai_name = '无'

    reback_dic = {'limit_up_time': limit_up_time,
                  'industry': industry,
                  'st_name': st_name,
                  'st_code': st_code,
                  'xgb_describtion': xgb_describtion,
                  'xgb_24H_data': xgb_24H_data,
                  'ehd_data': ehd_data,
                  'hde_data': hde_data,
                  'co_basic_data': co_basic_data,
                  'jygs_his_data': jygs_his_data,
                  'bankuai_name': bankuai_name,
                  'gain_describtion': gain_describtion}
    return [st_name, st_code, industry, xgb_describtion, xgb_24H_data, ehd_data, hde_data, co_basic_data], reback_dic


def limit_up_analyse_v2(api_key, model_name, text):
    chat = ChatOpenAI(temperature=0, openai_api_key=api_key, model=model_name)
    human_template = '''你是一个专业的股票涨停投资分析顾问，你的职责是针对股票市场的涨停板情况进行深度研究与分析，为投资者提供专业、精确的建议。

下面我会给你一些近期和股票涨停有关的信息，请你从我给出的信息中描述，分析并概括股票涨停的原因，并给出五条以内的结论。

需要满足以下几个条件：

- 结论要来源于我给的信息，不要出现信息以外的内容。
- 确保回答概括准确，逻辑清晰，分析专业。具体地，先列出事实，再进行分析。
- 注重信息与信息间的联系，综合多条信息挖掘潜在趋势。
- 如有时间等信息，请罗列出具体时间。
- 结论以换行符隔开，不要输出其他信息

比如，结论可以是：

1、 2023年第一季度百强地产新房销售同比转正，地产宽松政策成效逐步显现。
2、 2022年实现营业收入同比增长52.01%；实现归属于母公司净利润同比下降9.06%。拟向全体股东派发2022年全年现金红利每10股1.19元（含税），共计派发1.03亿元。
3、 报告显示，数据港已在北至乌兰察布、张北，南至广东深圳、河源，在“东数西算”的京津冀、长三角、粤港澳大湾区的枢纽节点上共建设运营35个数据中心，截至2022年12月31日运营IT兆瓦数达371MW，折算成5千瓦(KW)标准机柜约7.42万个，可以支撑大客户每秒200亿亿次运算量级的算力，可广泛支持人工智能、AIGC等领域发展。

下面是近期和股票涨停有关的信息：
{text}
    '''
    human_message_prompt = HumanMessagePromptTemplate.from_template(
        human_template)
    chat_prompt = ChatPromptTemplate.from_messages([human_message_prompt])
    chain = LLMChain(llm=chat, prompt=chat_prompt)
    content = chain.run(text=text)
    return content


# 1.不要有可能是什么涨停的原因之一
# 2.过于久远数据请进行剔除
###
# 1.涨停时间
# 2.韭研公社第一句的个股涨停原因 单独拆出来
# 3.韭研公社 最近一条的涨停结论 ->text
# dict
def limit_up_analyse(api_key, model_name, text):
    chat = ChatOpenAI(temperature=0, openai_api_key=api_key, model=model_name)
    human_template = '''你是一个专业的股票涨停投资分析顾问，你的职责是针对股票市场的涨停板情况进行深度研究与分析，为投资者提供专业、精确的建议。

下面我会给你一些近期和股票涨停有关的信息，请你从我给出的信息中描述，分析并概括股票涨停的原因，并给出五条以内的结论。

需要满足以下几个条件：

- 结论要来源于我给的信息，不要出现信息以外的内容。
- 确保回答概括准确，逻辑清晰，分析专业。具体地，先列出事实，再进行分析。
- 注重信息与信息间的联系，综合多条信息挖掘潜在趋势。
- 如有时间等信息，请罗列出具体时间。
- 结论以换行符隔开，不要输出其他信息
- 每个结论结尾不要以“这可能是推动其股票涨停的因素”这种话术为结尾

比如，结论可以是：

1、 2023年第一季度百强地产新房销售同比转正，地产宽松政策成效逐步显现。
2、 2022年实现营业收入同比增长52.01%；实现归属于母公司净利润同比下降9.06%。拟向全体股东派发2022年全年现金红利每10股1.19元（含税），共计派发1.03亿元。
3、 报告显示，数据港已在北至乌兰察布、张北，南至广东深圳、河源，在“东数西算”的京津冀、长三角、粤港澳大湾区的枢纽节点上共建设运营35个数据中心，截至2022年12月31日运营IT兆瓦数达371MW，折算成5千瓦(KW)标准机柜约7.42万个，可以支撑大客户每秒200亿亿次运算量级的算力，可广泛支持人工智能、AIGC等领域发展。

下面是近期和股票涨停有关的信息：
{text}
    '''
    logger.info(f'[涨停分析] 输入分析文字长度 {len(human_template)}')
    human_message_prompt = HumanMessagePromptTemplate.from_template(
        human_template)
    chat_prompt = ChatPromptTemplate.from_messages([human_message_prompt])
    chain = LLMChain(llm=chat, prompt=chat_prompt)
    content = chain.run(text=text)
    return content


def limit_up_title(api_key, model_name, text):
    chat = ChatOpenAI(temperature=0, openai_api_key=api_key, model=model_name)
    human_template = '''你是一个专业的股票涨停投资分析顾问，你的职责是针对股票市场的涨停板情况进行深度研究与分析，为投资者提供专业、精确的建议。
下面我会给你一些近期与该股票涨停有关的信息，请你从我给出的信息中描述，分析并凝练股票所属板块及板块异动的原因。
需要满足以下几个条件：
- 结论要来源于我给的信息，不要出现信息以外的内容。
- 确保回答概括准确，逻辑清晰，分析专业。
- 注重信息与信息间的联系，综合多条信息挖掘潜在趋势。
- 不要输出其他信息。
- 输出的信息要足够精炼。
- 输出格式应是"板块的名字；板块异动的原因"，板块的名字部分填充该股所属板块的名称，板块异动的原因填充该股所属板块异动的原因。

以下是股票所属板块及板块异动的原因的输出样式，请严格按照以下目标格式输出结果：

1、 车载显示；Mini LED车载显示市场需求将迎来大规模增长。
2、 半导体；摩根士丹利指出，随晶圆厂产能利用率下滑，进入今年下半年后很快就转为库存去化，加上需求复苏，到明年上半年将再现芯片短缺。
3、 液冷超充；国庆期间，华为数字能源助力打造的318川藏超充绿廊的多个全液冷超充站正式上线。

下面是近期与该股票涨停有关的信息：
{text}
    '''
    logger.info(f'[涨停分析] 输入分析文字长度 {len(human_template)}')
    human_message_prompt = HumanMessagePromptTemplate.from_template(
        human_template)
    chat_prompt = ChatPromptTemplate.from_messages([human_message_prompt])
    chain = LLMChain(llm=chat, prompt=chat_prompt)
    content = chain.run(text=text)
    return content


def check_if_text_empty(text):
    if text != '':
        return False
    else:
        return True


def to_analyse_v1_text(data_list, data_dic, model_name, api_key, jh_text):
    # KEY: xgb_describtion
    # data_list [st_name, st_code, industry, xgb_describtion, xgb_24H_data, ehd_data, hde_data, co_basic_data]
    # reback_dic = {'limit_up_time':limit_up_time,
    #               'industry':industry,
    #               'st_name':st_name,
    #               'st_code':st_code,
    #               'xgb_describtion':xgb_describtion,
    #               'xgb_24H_data':xgb_24H_data,
    #               'ehd_data':ehd_data,
    #               'hde_data':hde_data,
    #               'co_basic_data':co_basic_data,
    #               'jygs_his_data':jygs_his_data,
    #               'bankuai_name':bankuai_name,
    #               'gain_describtion':gain_describtion}
    # st_name = data_list[0]
    # st_code = data_list[1]
    st_name = data_dic['st_name']
    st_code = data_dic['st_code']
    logger.info(f'[涨停分析] analying v1  {st_name}')
    industry = ""
    # for data in data_list[2]:
    for data in data_dic['industry']:
        for key in data.keys():
            industry += data[key]
            industry += '\n'
    # industry = ''.join([data[key] for data in data_list[2] for key in data.keys()])
    xgb_describtion = ""
    # for data in data_list[3]:
    for data in data_dic['xgb_describtion']:
        for key in data.keys():
            xgb_describtion += data[key]
            xgb_describtion += '\n'
    xgb_24H_data = ""
    # for data in data_list[4]:
    for data in data_dic['xgb_24H_data']:
        for key in data.keys():
            if data[key] == "":
                pass
            else:
                xgb_24H_data += data[key] + ' '
        xgb_24H_data += '\n'
    ehd_data = ""
    # for data in data_list[5]:
    for data in data_dic['ehd_data']:
        for key in data.keys():
            if key == 'mes_time':
                ehd_data += data[key]
                ehd_data += '\n'
            if key == 'msg_q':
                ehd_data += 'Q:'
                ehd_data += data[key]
                ehd_data += '\n'
            if key == 'msg_a':
                ehd_data += 'A:'
                ehd_data += data[key]
                ehd_data += '\n'
    hde_data = ""
    # for data in data_list[6]:
    for data in data_dic['hde_data']:
        for key in data.keys():
            if key == 'mes_time':
                hde_data += data[key]
                hde_data += '\n'
            if key == 'msg_q':
                hde_data += 'Q:'
                hde_data += data[key]
                hde_data += '\n'
            if key == 'msg_a':
                hde_data += 'A:'
                hde_data += data[key]
                hde_data += '\n'
    co_basic_data = ""
    # for data in data_list[7]:
    for data in data_dic['co_basic_data']:
        for key in data.keys():
            co_basic_data += data[key]
            co_basic_data += '\n'
    # analyse jygs
    # jygs_his = data_dic['jygs_his_data']
    # select * from spiders_jiuyangongshe where stock_code like '%{}%'  order by article_create_time desc limit 1
    former_limit_up_reason_title = []
    former_limit_up_reason = ""
    former_limit_up_block_reason = ""
    former_limit_up_info_time = ""
    for data in data_dic['jygs_his_data']:
        split_n = data['expound'].split('\n', 1)
        former_limit_up_reason_title = split_n[0].split('+')
        former_limit_up_reason = split_n[1]
        former_limit_up_block_reason = data['reason']
        former_limit_up_info_time = data['article_create_time']
    bankuai_name = data_dic['bankuai_name']
    gain_describtion = data_dic['gain_describtion']
    limit_up_time = data_dic['limit_up_time']
    former_limit_up_reason_title_text = ','.join(former_limit_up_reason_title)
    datadata_dic = {'st_name': st_name,  # 1
                    'st_code': st_code,  # 1
                    "industry": industry,  # 1
                    "xgb_describtion": xgb_describtion,  # 1
                    "xgb_24H_data": xgb_24H_data,  # 1
                    "ehd_data": ehd_data,  # 1
                    "hde_data": hde_data,  # 1
                    "co_basic_data": co_basic_data,  # 1
                    "former_limit_up_reason_title_text": former_limit_up_reason_title_text,  # 1 text
                    "former_limit_up_reason": former_limit_up_reason,  # 1 text
                    'bankuai_name': bankuai_name,  # text
                    'gain_describtion': gain_describtion,  # 1 选股宝板块涨停描述
                    'former_limit_up_block_reason': former_limit_up_block_reason,  # 1 韭研公社板块涨停原因
                    'limit_up_time': limit_up_time,  # 1 涨停时间
                    'former_limit_up_info_time': former_limit_up_info_time}
    datadata_list = [st_name, st_code, industry, xgb_describtion,
                     xgb_24H_data, ehd_data, hde_data, co_basic_data]

    text = f'''
{datadata_dic['st_name']} {datadata_dic['st_code']}
所属行业：{datadata_dic['industry']}
来源1:
该股曾经异动的时间: {datadata_dic['former_limit_up_info_time']}
该股曾经板块异动的关键词:{datadata_dic['former_limit_up_reason_title_text']}
该股当日涨停时间：{datadata_dic['limit_up_time']}

涨停基本面上的解读:
{datadata_dic['xgb_describtion']}
'''
    if datadata_dic['gain_describtion'] != '' or datadata_dic['former_limit_up_block_reason'] != '':
        text += f'''
所属板块的解读：
{datadata_dic['gain_describtion']}

{datadata_dic['former_limit_up_info_time']}
{datadata_dic['former_limit_up_block_reason']}
'''
    if datadata_dic['xgb_24H_data'] != '':
        text += f'''
快讯:
{datadata_dic['xgb_24H_data']}
'''
    if datadata_dic['former_limit_up_reason'] != '':
        text += f'''
来源3:
{datadata_dic['former_limit_up_info_time']}
{datadata_dic['former_limit_up_reason']}
'''
    text += f'''
来源4:
{datadata_dic['ehd_data']}

来源5:
{datadata_dic['hde_data']}

来源6:
公司基本信息：{datadata_dic['co_basic_data']}
'''
    text = text[:3000]
    logger.info(f'[涨停分析] {text}')
    no_need_gen_tittle_flag = False
    try:
        specified_time = datetime.strptime(
            datadata_dic['former_limit_up_info_time'], '%Y-%m-%d %H:%M:%S')
        current_time = datetime.now()
        # 韭研公社数据时间小于14天
        if current_time - specified_time < timedelta(days=14):
            # 并且存在相关数据
            title_gen = f'''
{datadata_dic['st_name']} {datadata_dic['st_code']}
该股属于的板块：{datadata_dic['bankuai_name']}
该股曾经涨停的时间及关键词: {datadata_dic['former_limit_up_info_time']} {datadata_dic['former_limit_up_reason_title_text']}
'''
            if datadata_dic['gain_describtion'] != '':
                title_gen += f'''该股当天所属板块的异动：{datadata_dic['gain_describtion']}
'''
            if datadata_dic['former_limit_up_block_reason'] != '':
                title_gen += f'''该股曾经涨停板块异动原因：{datadata_dic['former_limit_up_info_time']} {datadata_dic['former_limit_up_block_reason']}
'''
            if datadata_dic['xgb_24H_data'] != '':
                title_gen += f'''快讯:{datadata_dic['xgb_24H_data']}
'''
        else:
            if datadata_dic['xgb_24H_data'] == "" and datadata_dic['gain_describtion'] == "":
                no_need_gen_tittle_flag = True
                title_gen = datadata_dic['bankuai_name'] + ';'
            else:
                no_need_gen_tittle_flag = False
                title_gen = f'''
{datadata_dic['st_name']} {datadata_dic['st_code']}
该股属于的板块：{datadata_dic['bankuai_name']}
该股当天所属板块的异动：{datadata_dic['gain_describtion']}
快讯:{datadata_dic['xgb_24H_data']}
'''
    except Exception as e:
        logger.info(f'[涨停分析] no data error! But continue {e}')
        if datadata_dic['xgb_24H_data'] == "" and datadata_dic['gain_describtion'] == "":
            no_need_gen_tittle_flag = True
            title_gen = datadata_dic['bankuai_name'] + ';'
        else:
            no_need_gen_tittle_flag = False
            title_gen = f'''
{datadata_dic['st_name']} {datadata_dic['st_code']}
该股属于的板块：{datadata_dic['bankuai_name']}
该股当天所属板块的异动：{datadata_dic['gain_describtion']}
快讯:{datadata_dic['xgb_24H_data']}
'''
    if no_need_gen_tittle_flag is True:
        logger.info('[涨停分析] 无相关数据 不生成板块异动')
        title = title_gen
    else:
        logger.info('[涨停分析] 生成板块异动')
        # gpt-3.5-turbo-16k
        title = limit_up_title(api_key, model_name, title_gen)
    content = limit_up_analyse(api_key, model_name, text)
    # 去生成的文本 to sql
    text_sql = title_gen + '\n\n' + text
    # 已生成的文本 to sql
    content_sql = title + '\n\n' + jh_text + content
    today = datetime.now().strftime("%Y-%m-%d")
    inser_save_text_sql = """insert into sql_analyse_text_v1v2 (st_code, v1_input_text, v2_input_text, date , v1_text,v2_text) 
    values (%s,%s,'',%s,%s,'')"""
    db.insertone(inser_save_text_sql, [
                 datadata_dic['st_code'], text_sql, today, content_sql])
    return content_sql


def run():
    query = []
    today = datetime.now().strftime("%Y-%m-%d")
    model_name = OPENAI_LIMIT_UP_MODEL
    if len(query) == 1:
        pass
    elif len(query) == 0:
        sql = "select * from spider_status where date = '{}' ORDER BY query_index ASC".format(
            today)
        results = db.select_all_dict(sql)
        for result in results:
            query_lenth_flag = len(query)
            if query_lenth_flag >= 1:
                pass
            # 已暂停 1020
            elif result['all_status'] == '1020':
                pass
            elif result['analy_first_finish_flag'] == False and result['processing_flag'] == False:
                query.append(result)  # --->v1_text
            # 待分析 1041
            elif result['analy_first_finish_flag'] == True and result['jygs_statu'] == '1041' and result[
                    'processing_flag'] == False and result['all_status'] == '1031':
                query.append(result)  # --->v2_text
        logger.info(f'[涨停分析] 分析队列{query}')
    if len(query) == 0:
        # 如果无效 则清除flag
        update_sql_query_index = "UPDATE spider_status SET processing_flag = False WHERE date='{}';".format(
            today)
        db.insertone(update_sql_query_index)
        pass
    else:
        # print(query[0].keys())
        # print(query[0])
        #
        # ---> gen v1 text
        try:
            api_key = get_model_token(model_name)
            if query[0]['analy_first_finish_flag'] == False and query[0]['processing_flag'] == False:
                logger.info(f"[涨停分析]  {query[0]['st_code']}  开始分析v1")
                update_sql_query_index = "UPDATE spider_status SET processing_flag = TRUE WHERE st_code = '{}' and date='{}';".format(
                    query[0]['st_code'], today)
                db.insertone(update_sql_query_index)
                logger.info(
                    f"[涨停分析]  {query[0]['st_code']}  v1 flag processing update ok")
                data_list, data_dic = to_collect_v1_data(query[0])
                sql_jh_add = "select report_date_name,totaloperatereve, totaloperaterevetz, kcfjcxsyjlr,  kcfjcxsyjlrtz from spider_eastmoney_company_report_info where security_code='{}' order by report_date desc limit 1;".format(
                    query[0]['st_code'])
                jh_result = db.select_one_dict(sql_jh_add)
                try:
                    jh_text = """该公司{}实现营业总收入{:,.1f}元，营业总收入同比增长{:,.2f}%，实现扣非净利润{:,.1f}元，实现扣非净利润同比增长{:,.2f}%。\n\n""".format(
                        jh_result['report_date_name'], jh_result['totaloperatereve'], jh_result['totaloperaterevetz'],
                        jh_result['kcfjcxsyjlr'],
                        jh_result['kcfjcxsyjlrtz'])
                except Exception as e:
                    jh_text = "\n\n"
                check_if_in_db = "select v1_text from sql_analyse_text_v1v2 where st_code like '%{}%' and date='{}' and v1_text <> ''".format(
                    query[0]['st_code'], today)
                check_if_in_db_result = db.select_all_dict(check_if_in_db)
                if len(check_if_in_db_result) > 0:
                    v1_content = check_if_in_db_result[0]['v1_text']
                else:
                    v1_content = to_analyse_v1_text(
                        data_list, data_dic, model_name, api_key, jh_text)
                for key in ['nb_statu', 'xgb_statu', 'ehd_statu', 'hdy_statu']:
                    # 待分析 1041 已分析 1042
                    if query[0][key] == '1041':
                        query[0][key] = '1042'
                query[0]['processing_flag'] = False
                # query[0]['date'] = today
                # all_status v1已处理
                query[0]['all_status'] = "1031"
                query[0]['analy_first_finish_flag'] = True
                # jh_text
                query[0]['v1_text'] = v1_content
                logger.info(f"[涨停分析]  {query[0]}")
                checksql, checkdata = to_update_v1_text_statu(query[0])
                logger.info(f"[涨停分析]  {query[0]['st_code']}  已分析v1")
                to_check_v1_text_statu(query[0], checksql, checkdata)
                logger.info(f"[涨停分析]  {query[0]['st_code']}  v1状态更新finish")
            # # ---> gen v2 text
            # 待分析 1041
            elif query[0]['analy_first_finish_flag'] == True and query[0]['jygs_statu'] == '1041' and query[0][
                    'processing_flag'] == False and query[0]['all_status'] == '1031':
                logger.info(f"[涨停分析]  go v2")
                update_sql_query_index = "UPDATE spider_status SET processing_flag = TRUE WHERE st_code = '{}' and date='{}';".format(
                    query[0]['st_code'], today)
                db.insertone(update_sql_query_index)
                logger.info(f"[涨停分析]  v2 flag processing update ok")
                data_list = to_collect_v2_data(query[0])
                sql_jh_add = "select report_date_name,totaloperatereve, totaloperaterevetz, kcfjcxsyjlr,  kcfjcxsyjlrtz from spider_eastmoney_company_report_info where security_code='{}' order by report_date desc limit 1;".format(
                    query[0]['st_code'])
                jh_result = db.select_one_dict(sql_jh_add)
                try:
                    jh_text = """该公司{}实现营业总收入{:,.1f}元，营业总收入同比增长{:,.2f}%，实现扣非净利润{:,.1f}元，实现扣非净利润同比增长{:,.2f}%。\n\n""".format(
                        jh_result['report_date_name'], jh_result['totaloperatereve'], jh_result['totaloperaterevetz'],
                        jh_result['kcfjcxsyjlr'],
                        jh_result['kcfjcxsyjlrtz'])
                except Exception as e:
                    jh_text = ""

                check_v2_if_in_db = "select v2_text from sql_analyse_text_v1v2 where st_code like '%{}%' and date='{}' AND v2_text <> ''".format(
                    query[0]['st_code'], today)
                check_v2_if_in_db_result = db.select_all_dict(
                    check_v2_if_in_db)
                if len(check_v2_if_in_db_result) > 0:
                    v2_content = check_v2_if_in_db_result[0]['v2_text']
                else:
                    v2_content = to_analyse_v2_text(
                        data_list, model_name, api_key)
                # v2_content = to_analyse_v2_text(data_list, model_name, api_key)
                # for key in ['jygs_statu']:
                #     print(key)
                #     # 待分析 1041 已分析 1042
                #     if query[0][key] == '1041':
                #         query[0][key] ='1042'
                #         print('ok')
                query[0]['processing_flag'] = False
                # query[0]['date'] = today
                # v2已处理 1032
                query[0]['jygs_statu'] = "1042"
                query[0]['all_status'] = "1032"
                query[0]['analy_first_finish_flag'] = True
                query[0]['analy_second_finish_flag'] = True
                query[0]['v2_text'] = jh_text + v2_content
                logger.info(f"[涨停分析]  {query[0]['jygs_statu']}")
                checksql, checkdata = to_update_v2_text_statu(query[0])
                logger.info(f"[涨停分析]  {query[0]['st_code']}")
                to_check_v2_text_statu(query[0], checksql, checkdata)
                logger.info(f"[涨停分析]  v2状态更新finish")
            query.pop(0)
        except Exception as e:
            # exc_info = traceback.format_exc()
            # print(exc_info)
            logger.error(f'next pass {e}')
            pass
