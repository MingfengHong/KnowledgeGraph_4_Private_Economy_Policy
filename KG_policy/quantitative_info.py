import pandas as pd
from openai import AsyncOpenAI  # 保持 AsyncOpenAI
import asyncio  # 保持 asyncio
import time  # 保持 time

# --- 配置信息 ---
API_KEY = "sk-xxx"  # !!! 用户提供的API Key !!!
BASE_URL = "https://api.deepseek.com"
INPUT_CSV_FILE = "policy_data_standardized_v4_extracted_v2.csv"  # 输入文件名
OUTPUT_CSV_FILE = "policy_data_with_quantitative_info_v6_formatted.csv"  # 输出文件名 (更新版本号和描述)
CONCURRENCY_LIMIT = 5  # 并发API调用限制

# --- PolicyTool 到 QuantitativeInfo 格式的映射 ---
# (映射表内容必须完整)
policy_tool_to_format_map = {
    "税额基数扣减": "[金额]X万元; [比例]Y%",
    "税额加计扣除": "[比例]X%",
    "税率式减免（优惠税率）": "[税率]X%",
    "税额式减免（直接减免额）": "[金额]X万元",
    "即征即退/先征后返（退税）": "[比例]X%; [金额]Y万元",
    "税收抵免/抵扣应纳税额": "[金额]X万元; [比例]Y%",
    "税款缓缴（延期缴纳）": "[期限]X个月 (或 [期限]Y年)",
    "加速折旧/摊销": "[缩短后年限]X年; [摊销比例]Y%; [一次性计提设备价值上限]Z万元",
    "亏损结转弥补期限延长": "[延长后总年限]X年",
    "行政事业性收费减免": "[减免比例]X%; [减免金额]Y万元; [免征项目数量]Z项",
    "财政直接补贴": "[补贴金额]X万元; [补贴比例]Y%",
    "财政专项奖励": "[奖励金额]X万元",
    "政府投资基金股权投入": "[投入金额]X万元; [占股比例]Y%",
    "政府采购价格扣除优惠": "[价格扣除比例]X%",
    "政府采购份额预留": "[预留份额比例]X%",
    "员工激励计划税收优惠": "[激励额度上限]X万元; [优惠税率]Y%",
    "突发事件专项财政纾困直补": "[补贴总金额]X万元; [人均补贴金额]Y元",
    "贷款风险代偿基金": "[基金规模]X亿元; [代偿比例上限]Y%",
    "贷款损失风险补偿": "[补偿比例]X%; [单户补偿金额上限]Y万元",
    "贷款利息补贴 (财政贴息)": "[贴息率]X%; [贴息金额上限]Y万元; [贴息年限]Z年",
    "融资担保费率补贴": "[担保费补贴比例]X%; [补贴后费率上限]Y%",
    "融资担保风险分担机制": "[政府分担比例]X%; [银行分担比例]Y%; [担保机构分担比例]Z% (或统一为 [风险分担比例组合]X:Y:Z)",
    "应急转贷（过桥）资金服务": "[资金池规模]X亿元; [单笔使用额度上限]Y万元; [日费率上限]Z‰; [使用期限上限]A天",
    "知识产权质押融资风险补偿": "[补偿比例]X%; [单笔贷款补偿上限]Y万元",
    "知识产权价值评估费用补贴": "[评估费补贴比例]X%; [单项评估补贴上限]Y万元",
    "供应链应付账款确权登记支持": "[年度目标确权金额]X亿元; [支持确权登记数量]Y笔",
    "信用风险缓释工具创设与推广": "[工具发行规模]X亿元; [创设工具数量]Y期",
    "债券发行“绿色通道”审批": "[审批时限压缩比例]X%; [审批时限缩短天数]Y个工作日",
    "创业投资引导基金参股": "[引导基金参股比例上限]X%; [引导基金出资金额上限]Y万元",
    "天使投资引导基金直接投资": "[单个项目投资金额上限]X万元",
    "引导基金投资退出让利机制": "[投资收益让利比例]X%",
    "出口信用保险保费补贴": "[保费补贴比例]X%",
    "出口信用保险承保风险补偿": "[风险补偿比例]X%; [风险补偿金额上限]Y万元",
    "市场准入负面清单制度": "[清单事项压减数量]X项; [清单事项压减比例]Y%",
    "政策措施公平竞争审查机制": "[政策审查覆盖率]X%; [存量政策审查完成比例]Y%",
    "反垄断执法调查与处罚": "[罚款金额]X亿元; [上一年度销售额罚款比例]Y%",
    "反不正当竞争行为执法查处": "[罚没金额]X万元; [查处案件数量]Y起",
    "隐性市场壁垒专项清理": "[清理壁垒数量]X项",
    "妨碍统一市场政策规定废止": "[废止规定数量]X件",
    "产权界定与登记公示制度": "[特定产权登记完成率]X%; [登记覆盖范围描述]Y类产权",
    "涉企产权案件甄别与纠错机制": "[甄别纠错案件数量]X起; [纠错案件占同类案件比例]Y%",
    "知识产权侵权惩罚性赔偿": "[赔偿倍数下限]X倍; [赔偿倍数上限]Y倍",
    "“监管沙盒”试点": "[试点项目数量]X个; [入盒测试企业数量]Y家",
    "科技创新券（服务购买凭证）": "[创新券面额]X万元/张; [企业年度申领上限金额]Y万元; [服务费用抵扣比例]Z%",
    "创新平台建设财政资助": "[一次性建设资助金额]X万元; [按投资额资助比例]Y%",
    "创新平台建设要素优先保障": "[优先保障用地指标]X亩; [优先保障能耗指标]Y吨标准煤",
    "首创产品应用风险补偿": "[应用风险补偿比例]X% (基于采购金额); [风险补偿金额上限]Y万元; [首台套投保费用补贴比例]Z%",
    "首创产品政府采购倾斜": "[价格评审扣除比例]X%; [预留采购份额比例]Y%",
    "知识产权申请与维持资助": "[发明专利每件资助金额]X元; [企业年度资助上限数量]Y件",
    "高价值知识产权培育与转化奖励": "[单项转化奖励金额]X万元; [企业年度奖励上限金额]Y万元",
    "企业知识产权管理规范认证激励": "[首次贯标认证激励金额]X万元",
    "引进人才专项生活补贴": "[一次性生活补贴金额]X万元; [每月生活补贴金额]Y元; [补贴发放期限]Z年",
    "引进人才安居保障服务": "[提供人才公寓面积]X平方米; [购房补贴金额]Y万元; [每月租房补贴金额]Z元",
    "引进人才科研与创业项目资助": "[单个项目最高资助金额]X万元",
    "企业转型升级诊断咨询服务": "[诊断咨询费用补贴比例]X%; [单次服务补贴金额上限]Y万元",
    "技术改造与设备更新财政补贴": "[设备投资额补贴比例]X%; [单个项目补贴金额上限]Y万元",
    "参与标准制修订项目资助": "[主导国际标准资助金额]X万元; [主导国家标准资助金额]Y万元; [参与行业标准资助金额]Z万元",
    "标准制定信息与协调支持": "[年度信息通报会数量]X场; [服务覆盖企业数量]Y家",
    "创业孵化载体运营绩效奖励": "[年度优秀孵化器奖励金额]X万元; [每成功孵化高新技术企业奖励金额]Y万元",
    "创业孵化载体服务能力提升资助": "[服务平台建设资助金额上限]X万元; [按实际投入资助比例]Y%",
    "公共科研设施与数据资源开放共享目录": "[纳入目录仪器设备数量]X台/套; [开放共享数据量]Y TB",
    "企业使用公共研发资源费用补贴": "[实际使用费用补贴比例]X%; [年度补贴金额上限]Y万元/家",
    "行政许可事项清单管理": "[清单内行政许可事项数量上限]X项; [年度清单事项压减比例]Y%",
    "“一件事一次办”集成服务": "[集成服务事项数量]X个; [办理时限平均压缩比例]Y%; [平均办理时间]Z个工作日",
    "一体化在线政务服务平台": "[政务服务事项网上可办率]X%; [平台年度活跃用户数量]Y万户",
    "惠企政策“免申即享”兑现": "[纳入免申即享政策数量]X项; [年度兑现资金总额]Y亿元",
    "“双随机、一公开”监管": "[日常监管抽查比例下限]X%; [年度监管覆盖企业数量]Y万户",
    "跨部门联合检查机制": "[年度跨部门联合检查次数]X次; [联合检查占总检查比例]Y%",
    "新兴产业包容期与观察期监管": "[设定观察期限]X年 (或Y个月); [轻微违法首次免罚次数]Z次",
    "行政执法裁量权基准": "[具有明确裁量阶次的处罚事项比例]X%; [发布裁量基准的重点领域数量]Y个",
    "轻微违法行为依法不罚清单": "[清单包含不予处罚事项数量]X项",
    "拖欠账款投诉举报与处理渠道": "[投诉案件办结率]X%; [平均办结时间]Y个工作日",
    "恶意拖欠账款失信联合惩戒": "[年度纳入失信联合惩戒名单企业数量]X家; [实施联合惩戒措施项数]Y项",
    "防拖欠账款标准化合同条款推广": "[推广覆盖企业数量]X家; [规模以上工业企业合同采用率]Y%",
    "公益性法律咨询与援助": "[年度提供法律咨询服务人次]X人次; [年度法律援助案件数量]Y起",
    "企业合规管理体系建设指导": "[年度指导建设合规体系企业数量]X家; [组织培训场次]Y场",
    "商事纠纷多元化解决机制推广": "[诉前调解成功率]X%; [通过非诉方式解决纠纷占比]Y%",
    "优秀企业家宣传与荣誉表彰": "[年度表彰优秀企业家名额]X名; [主流媒体宣传报道篇数]Y篇",
    "企业家创新创业容错免责机制": "[明确可容错免责情形数量]X种; [适用容错免责案件比例]Y%",
    "惠企政策信息精准推送": "[政策信息推送覆盖率]X%; [目标企业触达数量]Y万户; [政策匹配精准度]Z%",
    "惠企政策“一站式”兑现服务": "[纳入一站式兑现政策数量]X项; [平均在线办理时间]Y分钟",
    "企业诉求统一受理与分办督办": "[企业诉求按期办结率]X%; [平均办理时限]Y个工作日",
    "重大涉企问题“一事一议”协调": "[年度协调解决重大问题数量]X个; [协调解决成功率]Y%",
    "政府购买专业化涉企公共服务": "[年度购买涉企公共服务项目数]X项; [年度购买服务总金额]Y万元",
    "中小企业公共服务平台运营支持": "[年度支持的示范平台数量]X个; [单个平台年度运营补贴金额]Y万元",
    "企业公共信用信息归集共享与公示": "[累计归集公共信用信息条数]X亿条; [实现数据共享的部门数量]Y个",
    "公共信用综合评价与差异化监管": "[年度完成信用评价企业数量]X万家; [A级信用企业激励措施比例]Y%; [对D级企业监管频次增加比例]Z%",
    "信用信息异议处理与信用修复": "[信用修复申请平均办结时限]X个工作日; [异议申诉成功率]Y%",
    "政府职能向社会组织转移清单": "[清单包含可转移职能事项数量]X项",
    "政府向社会组织购买服务规范": "[向社会组织购买服务占同类服务项目比例]X%; [年度购买服务资金总额]Y万元",
    "国际化经营合规指导与风险预警": "[年度发布国别贸易风险预警信息数]X条; [年度参与合规培训企业数]Y家",
    "国际贸易“单一窗口”服务": "[主要申报业务通过单一窗口应用率]X%; [货物申报业务覆盖率]Y%",
    "跨境贸易与投资审批流程优化": "[审批环节平均减少数量]X个; [审批时限平均压缩比例]Y%",
    "企业职工职业技能培训补贴": "[每人次培训补贴标准]X元; [年度补贴培训总人次]Y人次; [按培训费用补贴比例]Z%",
    "现代学徒制与新型学徒制推广": "[年度培养技能学徒数量]X名; [参与学徒制合作企业数量]Y家",
    "产业链供需对接平台服务": "[年度促成产业链对接项目数]X个; [促成对接项目金额]Y亿元",
    "产学研用协同创新项目资助": "[单个项目最高资助金额]X万元; [年度支持协同创新项目数量]Y个",
    "“标准地”供应制度": "[年度标准地供应面积]X亩; [标准地占新增工业用地比例]Y%; [约定容积率下限]Z; [亩均固定资产投资强度下限]A万元/亩",
    "项目审批“告知承诺制”": "[纳入告知承诺制审批事项数量]X项; [告知承诺制审批占同类审批比例]Y%"
}


async def extract_quantitative_info(aclient: AsyncOpenAI, tools_with_formats: list, full_text: str,
                                    semaphore: asyncio.Semaphore, original_row_index: int):
    """
    根据提供的政策工具列表及其各自的预期格式（组件模板），从政策全文中异步提取量化信息，
    并按照 `政策工具名称(组件1值, 组件2值, ...)` 的格式输出，不同工具间用分号空格分隔。
    """
    async with semaphore:
        if not tools_with_formats:
            return "没有可供处理的已知政策工具"  # Script-level status

        formatted_tool_list_string = []
        for i, (tool, fmt) in enumerate(tools_with_formats):
            # fmt在这里是组件提取的模板
            formatted_tool_list_string.append(f"工具{i + 1}名称: {tool}\n工具{i + 1}的组件提取模板: {fmt}")
        tools_and_formats_prompt_section = "\n\n".join(formatted_tool_list_string)

        system_prompt = """
你是一位专业的文本信息提取助手。
你的核心任务是从政策文本中，根据提供的一系列“政策工具”及其对应的“组件提取模板”，提取量化指标。
最终输出要求非常具体：
1. 对于每一个在文本中找到对应信息的政策工具，你需要生成一个 `政策工具名称(实际提取的组件值A, 实际提取的组件值B, ...)` 格式的字符串。其中，括号内的多个组件值之间用半角逗号和空格（", "）连接。
2. 如果文本中涉及多个政策工具并且都成功提取了信息，你需要将这些生成的 `政策工具名称(...)` 字符串用半角分号和空格（"; "）连接起来。
3. 如果通读全文后，对于所有提供的政策工具及其组件模板，都无法提取到任何有效的量化信息，则必须返回一个完全空的字符串。
请严格遵循这些格式化指令，不要添加任何额外的解释或标签。
"""
        user_prompt = f"""
政策文本:
---
{full_text}
---

当前政策可能涉及以下一项或多项政策工具。您的任务是从上述政策文本中，针对下面列出的每个工具及其“组件提取模板”，提取相关的量化指标，并严格按照最终输出格式要求进行组织。

可应用的政策工具及其组件提取模板列表:
=====================================
{tools_and_formats_prompt_section}
=====================================

请严格按照以下指示操作以生成最终输出：
1.  **自行判断适用性**：对于列表中的每一个“政策工具”，首先判断它是否在政策文本中被提及，并且文本中是否包含了符合其“组件提取模板”的、可抽取的量化信息。不是列表中的所有工具都必须在文本中找到对应信息。

2.  **提取组件并格式化单个工具结果**：
    a.  如果某个“政策工具”适用，请根据其对应的“组件提取模板”从文本中提取一个或多个组件的实际值。组件模板（例如“[金额]X万元; [比例]Y%”）指明了要寻找的信息片段和它们的结构。
    b.  政策文本中可能并不包含模板中所有的组件信息，请只提取文本中实际存在的、与模板组件要求相符的部分。
    c.  将为这**一个政策工具**提取到的所有**组件的实际值**（例如“[金额]50万元”、“[比例]10%”）用**半角逗号和单个空格（", "）**组合起来。例如，如果提取到两个组件，则组合为：`[金额]50万元, [比例]10%`。
    d.  然后，将这个逗号分隔的组件信息字符串用圆括号括起来，并以该“政策工具”的准确名称作为前缀，形成最终的单个工具输出格式：`政策工具名称(逗号分隔的组件实际值字符串)`。例如：`税额基数扣减([金额]50万元, [比例]10%)`。

3.  **汇总多个工具的结果**：
    * 如果在政策文本中找到了多个不同政策工具的信息，并为它们都生成了步骤2.d中所述的 `政策工具名称(...)` 格式字符串，请将这些字符串用**半角分号和单个空格（"; "）**连接起来。例如：`税额基数扣减([金额]50万元, [比例]10%); 财政直接补贴([补贴金额]100万元)`。

4.  **无信息可提取的情况**：如果经过仔细分析，对于列表中的所有政策工具，均无法从政策文本中提取任何有效量化信息来构成步骤2.d所述的 `政策工具名称(...)` 格式的字符串，则必须返回一个**完全空的字符串**（例如，""），不要返回“未找到”或任何其他文字。

5.  **输出要求**：您的回答必须严格是按上述规则提取并组合的最终结果字符串（单一工具结果、多工具组合结果，或空字符串）。绝对禁止包含任何额外的解释、标题（如“提取结果:”）、标签、对话、或任何非最终结果的文本。

最终提取的字符串:
"""
        try:
            response = await aclient.chat.completions.create(
                model="deepseek-chat",  # 或您选择的模型
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                stream=False,
                temperature=0.0,  # 确保结果的确定性
                max_tokens=1024  # 增加 token 限制以容纳更复杂的组合输出和 ToolName 前缀
            )
            raw_llm_output = response.choices[0].message.content
            print(f"  [Row {original_row_index + 1}] LLM原始输出: '{raw_llm_output}'")  # 打印原始LLM输出

            extracted_text = raw_llm_output.strip()
            # 根据Prompt，LLM应该直接返回空字符串。保险起见，如果返回"未找到"，也转为空字符串。
            if extracted_text == "未找到":
                return ""
            return extracted_text
        except Exception as e:
            tool_names_involved = ", ".join([t[0] for t in tools_with_formats])
            print(f"  [Row {original_row_index + 1}] 调用LLM时发生错误 (涉及工具: {tool_names_involved}): {e}")
            return "LLM调用错误"


async def main():
    print(f"正在从 {INPUT_CSV_FILE} 加载数据...")
    try:
        df = pd.read_csv(INPUT_CSV_FILE)
    except FileNotFoundError:
        print(f"错误: 输入文件 '{INPUT_CSV_FILE}' 未找到。")
        return
    except Exception as e:
        print(f"读取CSV文件时发生错误: {e}")
        return

    required_columns = ['PolicyTool', 'FullText']
    for col in required_columns:
        if col not in df.columns:
            print(f"错误: CSV文件必须包含 '{col}' 列。")
            return

    print(f"找到 {len(df)} 条政策需要处理。")

    # 新增列的名称修改
    output_column_name = "政策工具(量化)"
    df[output_column_name] = "未处理"

    aclient = AsyncOpenAI(api_key=API_KEY, base_url=BASE_URL)
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    tasks_to_run = []
    llm_processing_info = []

    for index, row in df.iterrows():
        policy_tool_string = row['PolicyTool']
        full_text_content = row['FullText']
        original_df_index = row.name

        if pd.isna(policy_tool_string) or not str(policy_tool_string).strip():
            df.loc[original_df_index, output_column_name] = "政策工具缺失或为空"
            continue
        if pd.isna(full_text_content) or not str(full_text_content).strip():  # 确保FullText也是字符串且非空
            df.loc[original_df_index, output_column_name] = "政策全文缺失或为空"
            continue

        # 确保full_text_content是字符串类型传递给LLM
        full_text_content_str = str(full_text_content)

        individual_tools = [tool.strip() for tool in str(policy_tool_string).split(';') if tool.strip()]
        if not individual_tools:
            df.loc[original_df_index, output_column_name] = "政策工具解析后为空"
            continue

        known_tools_with_formats_for_row = []
        unknown_tools_for_row = []
        for single_tool in individual_tools:
            if single_tool in policy_tool_to_format_map:
                known_tools_with_formats_for_row.append((single_tool, policy_tool_to_format_map[single_tool]))
            else:
                unknown_tools_for_row.append(single_tool)

        current_row_log_prefix = f"处理第 {original_df_index + 1}/{len(df)} 行 (DF索引 {original_df_index}): "
        if unknown_tools_for_row:
            print(
                f"{current_row_log_prefix}注意: 以下工具未在格式映射中定义，将忽略: {', '.join(unknown_tools_for_row)}")

        if known_tools_with_formats_for_row:
            print(f"{current_row_log_prefix}将向LLM传递 {len(known_tools_with_formats_for_row)} 个已知工具。")
            task = extract_quantitative_info(aclient, known_tools_with_formats_for_row, full_text_content_str,
                                             semaphore, original_df_index)
            tasks_to_run.append(task)
            llm_processing_info.append({'original_index': original_df_index})
        else:
            df.loc[original_df_index, output_column_name] = "未找到可处理的政策工具（均未在格式映射中定义或原始列表为空）"
            print(f"{current_row_log_prefix}无已知工具可处理。")

    if tasks_to_run:
        print(f"\n开始并发执行 {len(tasks_to_run)} 个LLM提取任务 (并发数上限: {CONCURRENCY_LIMIT})...")
        start_time = time.time()
        all_results = await asyncio.gather(*tasks_to_run, return_exceptions=True)
        end_time = time.time()
        print(f"所有LLM任务执行完毕，耗时: {end_time - start_time:.2f} 秒。")

        for i, result_or_exc in enumerate(all_results):
            original_df_idx = llm_processing_info[i]['original_index']
            if isinstance(result_or_exc, Exception):
                print(f"  [Row {original_df_idx + 1}] 任务执行时发生异常: {result_or_exc}")
                df.loc[original_df_idx, output_column_name] = "任务执行异常"  # 或者更具体的错误信息
            else:
                df.loc[original_df_idx, output_column_name] = result_or_exc
                # 打印一些结果样本，确认格式
                if (i + 1) % 5 == 0 or i == len(all_results) - 1:  # 每5个或最后一个打印进度
                    print(
                        f"  ...已赋值 {i + 1}/{len(all_results)} LLM结果。行 {original_df_idx + 1} (DF索引 {original_df_idx}) 结果: '{str(result_or_exc)[:200]}...'")
    else:
        print("没有需要通过LLM处理的任务。")

    # AsyncOpenAI client uses httpx.AsyncClient which should be closed if managed manually.
    # However, if aclient is not used in a context manager (`async with`),
    # explicit close is good practice.
    await aclient.close()

    print("\n所有行处理完毕。")
    print(f"正在将更新后的数据保存到 {OUTPUT_CSV_FILE}...")
    try:
        df.to_csv(OUTPUT_CSV_FILE, index=False, encoding='utf-8-sig')
        print(f"成功保存到 {OUTPUT_CSV_FILE}")
    except Exception as e:
        print(f"保存CSV文件时发生错误: {e}")


if __name__ == "__main__":
    # 简单的API Key占位符检查 (用户已提供实际key，此检查可能不再严格阻断)
    # if API_KEY == "<你的DeepSeek API Key>" or (API_KEY.startswith("sk-") and len(API_KEY) < 40): # 过于简单的key也警告
    #     print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    #     print("!!! 警告：请确保API_KEY已正确设置为您的有效API Key。 !!!")
    #     print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    #     # 对于脚本的继续执行，取决于您是否希望在占位符key存在时停止
    # else:
    #     asyncio.run(main())
    asyncio.run(main())  # 直接运行