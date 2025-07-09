import os
import pandas as pd
import openai  # openai library is used for DeepSeek
import time
import json

# --- DeepSeek API 配置 ---
DEEPSEEK_API_KEY = "sk-xxx" # <--- User-provided key, ensure it's intended or replace
DEEPSEEK_BASE_URL = "https://api.deepseek.com"
DEEPSEEK_MODEL = "deepseek-chat "

client = None
if DEEPSEEK_API_KEY != "YOUR_DEEPSEEK_API_KEY" and DEEPSEEK_API_KEY:
    try:
        client = openai.OpenAI(api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_BASE_URL)
    except Exception as e:
        print(f"Error initializing DeepSeek client: {e}")
        client = None
else:
    print("DeepSeek API Key is not set or is still the placeholder. API calls will be skipped.")

# --- 预定义的实体列表 ---
POLICY_TOPICS_LIST = [
    "融资畅通与成本压降", "税费负担减轻与政策优惠", "营商环境持续优化",
    "市场准入放宽与公平准营保障", "科技创新驱动与研发激励", "高素质人才引育与服务保障",
    "公平竞争市场秩序建设", "企业产权与合法权益有效保护", "数字化与智能化转型赋能",
    "“专精特新”与中小企业高质量发展", "创业服务体系建设与创新生态营造", "知识产权创造、运用与保护强化",
    "绿色低碳转型与可持续发展引导", "生产要素市场化配置与成本管理", "内外贸一体化与国际化经营拓展",
    "产业链供应链韧性与协同能力提升", "质量提升、品牌建设与标准引领", "弘扬和激发优秀企业家精神",
    "政府采购支持与公平待遇保障", "产业基础设施配套与运营优化", "现代企业制度建设与公司治理优化",
    "企业社会责任建设与ESG发展引导", "区域协调发展战略与民企参与激励", "企业合规经营与风险抵御能力建设",
    "大中小企业融通发展与产业集群培育", "数据要素赋能与数据资产价值化", "市场化退出机制健全与企业注销便利化",
    "政务服务便捷与效能提升", "新质生产力培育与发展促进"
]

POLICY_TOOLS_LIST = [
    "税额基数扣减", "税额加计扣除", "税率式减免（优惠税率）", "税额式减免（直接减免额）",
    "即征即退/先征后返（退税）", "税收抵免/抵扣应纳税额", "税款缓缴（延期缴纳）",
    "加速折旧/摊销", "亏损结转弥补期限延长", "行政事业性收费减免", "财政直接补贴",
    "财政专项奖励", "政府投资基金股权投入", "政府采购价格扣除优惠", "政府采购份额预留",
    "员工激励计划税收优惠", "突发事件专项财政纾困直补", "贷款风险代偿基金",
    "贷款损失风险补偿", "贷款利息补贴 (财政贴息)", "融资担保费率补贴", "融资担保风险分担机制",
    "应急转贷（过桥）资金服务", "知识产权质押融资风险补偿", "知识产权价值评估费用补贴",
    "供应链应付账款确权登记支持", "信用风险缓释工具创设与推广", "债券发行“绿色通道”审批",
    "创业投资引导基金参股", "天使投资引导基金直接投资", "引导基金投资退出让利机制",
    "出口信用保险保费补贴", "出口信用保险承保风险补偿", "市场准入负面清单制度",
    "政策措施公平竞争审查机制", "反垄断执法调查与处罚", "反不正当竞争行为执法查处",
    "隐性市场壁垒专项清理", "妨碍统一市场政策规定废止", "产权界定与登记公示制度",
    "涉企产权案件甄别与纠错机制", "知识产权侵权惩罚性赔偿", "“监管沙盒”试点",
    "科技创新券（服务购买凭证）", "创新平台建设财政资助", "创新平台建设要素优先保障",
    "首创产品应用风险补偿", "首创产品政府采购倾斜", "知识产权申请与维持资助",
    "高价值知识产权培育与转化奖励", "企业知识产权管理规范认证激励", "引进人才专项生活补贴",
    "引进人才安居保障服务", "引进人才科研与创业项目资助", "企业转型升级诊断咨询服务",
    "技术改造与设备更新财政补贴", "参与标准制修订项目资助", "标准制定信息与协调支持",
    "创业孵化载体运营绩效奖励", "创业孵化载体服务能力提升资助", "公共科研设施与数据资源开放共享目录",
    "企业使用公共研发资源费用补贴", "行政许可事项清单管理", "“一件事一次办”集成服务",
    "一体化在线政务服务平台", "惠企政策“免申即享”兑现", "“双随机、一公开”监管",
    "跨部门联合检查机制", "新兴产业包容期与观察期监管", "行政执法裁量权基准",
    "轻微违法行为依法不罚清单", "拖欠账款投诉举报与处理渠道", "恶意拖欠账款失信联合惩戒",
    "防拖欠账款标准化合同条款推广", "公益性法律咨询与援助", "企业合规管理体系建设指导",
    "商事纠纷多元化解决机制推广", "优秀企业家宣传与荣誉表彰", "企业家创新创业容错免责机制",
    "惠企政策信息精准推送", "惠企政策“一站式”兑现服务", "企业诉求统一受理与分办督办",
    "重大涉企问题“一事一议”协调", "政府购买专业化涉企公共服务", "中小企业公共服务平台运营支持",
    "企业公共信用信息归集共享与公示", "公共信用综合评价与差异化监管", "信用信息异议处理与信用修复",
    "政府职能向社会组织转移清单", "政府向社会组织购买服务规范", "国际化经营合规指导与风险预警",
    "国际贸易“单一窗口”服务", "跨境贸易与投资审批流程优化", "企业职工职业技能培训补贴",
    "现代学徒制与新型学徒制推广", "产业链供需对接平台服务", "产学研用协同创新项目资助",
    "“标准地”供应制度", "项目审批“告知承诺制”"
]

TARGET_BENEFICIARIES_LIST = [
    "各类民营企业", "中小微企业", "个体工商户", "科技型民营企业", "“专精特新”中小企业",
    "初创期科技企业", "高成长性民营企业", "大型骨干民营企业", "外向型民营企业",
    "制造业民营企业", "现代服务业民营企业", "商贸流通民营企业", "平台经济民营企业",
    "文化和旅游领域民营企业", "体育健康养老产业民营经营主体", "节能环保与绿色低碳产业民营企业",
    "数字经济核心产业民营企业", "返乡入乡创业企业", "乡村特色产业民营经营主体",
    "特定区域与欠发达地区民营企业", "重点用工群体吸纳民营企业", "转型升级中民营企业",
    "生产经营暂时困难民营企业", "民营上市及拟上市企业", "参与混合所有制改革的民营投资者",
    "新型农业经营主体", "民办社会事业机构", "参与“一带一路”建设民营企业",
    "劳动密集型民营企业", "退役军人创办企业", "吸纳残疾人就业民营企业", "青年创业企业"
]

INDUSTRY_FOCUS_LIST = [  # 国民经济行业分类一级分类
    "农、林、牧、渔业", "采矿业", "制造业", "电力、热力、燃气及水生产和供应业", "建筑业",
    "批发和零售业", "交通运输、仓储和邮政业", "住宿和餐饮业", "信息传输、软件和信息技术服务业",
    "金融业", "房地产业", "租赁和商务服务业", "科学研究和技术服务业",
    "水利、环境和公共设施管理业", "居民服务、修理和其他服务业", "教育",
    "卫生和社会工作", "文化、体育和娱乐业", "公共管理、社会保障和社会组织", "国际组织", "全行业"
]

GEOGRAPHIC_REGION_EXAMPLES = ["全国各省、自治区、直辖市、新疆生产建设兵团", "广西壮族自治区", "苏州市", "恩施土家族苗族自治州", "中国（海南）自由贸易试验区"]

ENTITY_DEFINITIONS = {
    "PolicyTopic": {
        "description": "政策内容主要涉及的促进民营经济发展的具体议题或领域。",
        "examples": POLICY_TOPICS_LIST,  # 使用预定义列表
        "instruction": "请从预定义的政策主题列表中选择一项或多项。如果涉及多项，请用半角分号“;”隔开各项。如果均不包含，请输出“其他（此处填写实际政策主题的简要描述）”。"
    },
    "PolicyTool": {
        "description": "政策为达成目标所采用的具体措施、手段或方法。",
        "examples": POLICY_TOOLS_LIST,  # 使用预定义列表
        "instruction": "请从预定义的政策工具列表中选择一项或多项。如果涉及多项，请用半角分号“;”隔开各项。如果均不包含，请输出“其他（此处填写实际政策工具的简要描述）”。"
    },
    "TargetBeneficiary": {
        "description": "政策主要面向的民营经济主体类型。",
        "examples": TARGET_BENEFICIARIES_LIST,  # 使用预定义列表
        "instruction": "请从预定义的受益对象列表中选择一项或多项。如果涉及多项，请用半角分号“;”隔开各项。如果均不包含，请输出“其他（此处填写实际受益对象的简要描述）”。"
    },
    "GeographicRegion": {
        "description": "政策适用的地理范围或颁布机构所属的行政区域。",
        "examples": GEOGRAPHIC_REGION_EXAMPLES,
        "instruction": "请提取政策适用的地理范围，并使用正式全称（例如：全国各省、自治区、直辖市、新疆生产建设兵团、广西壮族自治区、新疆生产建设兵团、苏州市、恩施土家族苗族自治州、中国（海南）自由贸易试验区等）。如果提及多个区域，请用半角分号“;”隔开。如果范围为“全国各省、自治区、直辖市、新疆生产建设兵团”，则不要再加分号输出其他地区。"
    },
    "IndustryFocus": {
        "description": "政策特别关注或倾斜的产业领域（如果政策有明确的行业指向，且该行业内民营企业是主要受益者）。",
        "examples": INDUSTRY_FOCUS_LIST,  # 使用预定义列表
        "instruction": "请从预定义的行业分类列表中选择一项或多项该政策侧重的行业。如果未明确，一般为“全行业”，请自行判断。输出为“全行业”时，则不要再加分号输出其他行业。如果涉及多项，请用半角分号“;”隔开各项。如果均不明确指向这些一级分类，请输出“其他（此处填写实际侧重行业的简要描述）”或留空。"
    }
}


def construct_single_entity_prompt(policy_title, policy_full_text, entity_name, entity_info):
    """
    为单个实体类型构建Prompt，包含预定义列表和特定指令。
    """
    example_list_str = ""
    if entity_name in ["PolicyTopic", "PolicyTool", "TargetBeneficiary", "IndustryFocus"]:
        example_list_str = "预定义的候选列表如下 (请不要在回答中包含序号):\n"
        for i, item in enumerate(entity_info["examples"]):
            example_list_str += f"- {item}\n"  # 移除序号
        example_list_str += "\n"

    prompt = f"""
请你扮演一位专业的政策分析助手。根据以下提供的政策标题和政策全文内容，仅提取与 "{entity_name}" 相关的信息。

实体类型 "{entity_name}" 定义如下:
描述: {entity_info["description"]}
{example_list_str}
指示: {entity_info["instruction"]}

请尽可能根据文本推断。只有文本为空或者或者完全没有提及任何相关信息，或者该实体类型不适用，才在JSON中返回一个空列表。
否则尽量不要返回空列表。

政策标题:
{policy_title}

政策全文内容:
{policy_full_text}

请严格按照以下JSON格式输出 (如果选择多项，请将它们放在一个字符串中并用半角分号 ";" 隔开，或者按指示输出"其他(...)")：
{{
  "{entity_name}": ["提取结果1;提取结果2"]
}}
或者，如果不在列表中 (根据指示):
{{
  "{entity_name}": ["其他（实际描述）"]
}}
或者，信息确实且无法推断/该实体类型不适用:
{{
  "{entity_name}": []
}}

JSON Output:
"""
    return prompt


def call_deepseek_api_for_entity(policy_title, policy_full_text, entity_name, entity_info, retries=3, delay=5):
    if not client:
        print(f"  - DeepSeek client not initialized. Skipping API call for {entity_name}.")
        return []

    prompt = construct_single_entity_prompt(policy_title, policy_full_text, entity_name, entity_info)

    for attempt in range(retries):
        try:
            response = client.chat.completions.create(
                model=DEEPSEEK_MODEL,
                messages=[
                    {"role": "system",
                     "content": f"你是一位专业的政策文本分析助手，需要从文本中提取指定的 '{entity_name}' 信息，并严格按照指示的JSON格式和内容要求返回。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,  # 更低的温度以获取更确定的、基于列表的分类结果
                max_tokens=1024,  # 允许返回较长的 "其他(...)" 或多个分号分隔的项
                stream=False
            )

            content = response.choices[0].message.content
            # 清理可能的Markdown代码块标记
            if content.strip().startswith("```json"):
                content = content.strip()[7:-3].strip()
            elif content.strip().startswith("```"):
                content = content.strip()[3:-3].strip()

            extracted_json = json.loads(content)
            extracted_values_raw = extracted_json.get(entity_name, [])

            # API 可能直接返回一个包含单个字符串（可能含分号）的列表，或者空列表
            if isinstance(extracted_values_raw, list):
                if not extracted_values_raw:  # 空列表
                    return []
                # 如果列表非空，我们期望的是单个字符串，这个字符串内部可能包含分号
                # 或者是一个 "其他(...)" 形式的字符串
                single_string_result = extracted_values_raw[0]
                if isinstance(single_string_result, str) and single_string_result.strip():
                    return [single_string_result.strip()]  # 返回包含这个处理后字符串的列表
                else:
                    return []  # 如果列表中的元素不是有效字符串
            elif isinstance(extracted_values_raw, str) and extracted_values_raw.strip():  # 模型有时可能不按列表格式返回
                return [extracted_values_raw.strip()]
            else:
                return []  # 其他意外情况

        except openai.RateLimitError as e:
            print(
                f"  - API速率限制错误 for {entity_name} (尝试 {attempt + 1}/{retries}): {e}. 等待 {delay * (attempt + 1)} 秒后重试...")
            time.sleep(delay * (attempt + 1))
        except openai.APIError as e:
            print(f"  - DeepSeek API 错误 for {entity_name} (尝试 {attempt + 1}/{retries}): {e}")
            if attempt == retries - 1: return []
            time.sleep(delay)
        except json.JSONDecodeError as e:
            print(f"  - JSON解析API响应时出错 for {entity_name} (尝试 {attempt + 1}/{retries}): {e}")
            print(f"  - 原始响应内容: {content if 'content' in locals() else 'N/A'}")
            if attempt == retries - 1: return []
            time.sleep(delay)
        except Exception as e:
            print(f"  - 调用API时发生未知错误 for {entity_name} (尝试 {attempt + 1}/{retries}): {e}")
            if attempt == retries - 1: return []
            time.sleep(delay)
    return []


def supplement_policy_data_with_deepseek(csv_file_path):
    print(f"尝试读取CSV文件: {csv_file_path}")
    try:
        df = pd.read_csv(csv_file_path)
        print(f"成功读取 {len(df)} 条数据。")
    except FileNotFoundError:
        print(f"错误: 文件 '{csv_file_path}' 未找到.")
        return None
    except Exception as e:
        print(f"读取CSV文件 '{csv_file_path}' 时发生错误: {e}")
        return None

    new_df_columns = {}
    for entity_name in ENTITY_DEFINITIONS.keys():
        df_col_name = f"{entity_name}_extracted"
        new_df_columns[entity_name] = df_col_name
        if df_col_name not in df.columns:
            df[df_col_name] = pd.Series([None for _ in range(len(df))], dtype=object)  # 初始化为None，后续存列表
        else:  # 如果列已存在，准备重新填充
            df[df_col_name] = df[df_col_name].apply(lambda x: None)

    for index, row in df.iterrows():
        print(f"\n正在处理第 {index + 1}/{len(df)} 条政策: {row.get('标题', '无标题')}")

        policy_title = str(row.get('标题', '')) if pd.notna(row.get('标题')) else ""
        policy_full_text = str(row.get('全文内容', '')) if pd.notna(row.get('全文内容')) else ""

        if not policy_full_text and not policy_title:
            print("  - 政策标题和全文内容均为空，跳过API调用。")
            for entity_name in ENTITY_DEFINITIONS.keys():
                df.loc[index, new_df_columns[entity_name]] = []  # 存空列表
            continue

        if not client:
            print("  - DeepSeek API Key 未正确配置或客户端初始化失败，跳过所有API调用。")
            for entity_name in ENTITY_DEFINITIONS.keys():
                df.loc[index, new_df_columns[entity_name]] = []  # 存空列表
            continue

        for entity_name, entity_info in ENTITY_DEFINITIONS.items():
            print(f"  Extracting {entity_name}...")
            # API现在应该返回一个列表，其中包含一个字符串（该字符串可能包含分号或 "其他(...)"）
            extracted_list = call_deepseek_api_for_entity(policy_title, policy_full_text, entity_name, entity_info)

            if extracted_list:  # extracted_list 是一个列表，例如 ["结果1;结果2"] 或 ["其他(...)"]
                print(f"    - Extracted for {entity_name}: {extracted_list[0]}")  # 打印列表中的字符串
                df.loc[index, new_df_columns[entity_name]] = extracted_list[0]  # 直接存储这个字符串
            else:
                print(f"    - No information extracted for {entity_name}.")
                df.loc[index, new_df_columns[entity_name]] = ""  # 存空字符串

            time.sleep(3)

    return df


# --- 主程序 ---
if __name__ == "__main__":
    if DEEPSEEK_API_KEY == "YOUR_DEEPSEEK_API_KEY" or not DEEPSEEK_API_KEY:
        print("*****************************************************************")
        print("警告: DEEPSEEK_API_KEY 未设置或仍为占位符。")
        print("请在脚本中将 'YOUR_DEEPSEEK_API_KEY' 替换为您的真实密钥。")
        print("脚本将继续运行，但不会进行实际的API调用。")
        print("*****************************************************************")

    input_csv_path = r"C:\Users\hongm\OneDrive\桌面\民营经济促进政策\政策文本\policy_data_standardized_v4.csv"

    print(f"开始处理文件: {input_csv_path}")
    updated_df = supplement_policy_data_with_deepseek(input_csv_path)

    if updated_df is not None:
        output_csv_path = r"C:\Users\hongm\OneDrive\桌面\民营经济促进政策\政策文本\policy_data_standardized_v4_extracted_v2.csv"
        try:
            updated_df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
            print(f"\n处理完成！补充了信息的政策数据已保存到: {output_csv_path}")
            print("\n部分结果预览:")

            # 修正部分：重新构建预览列名列表
            extracted_column_names = [f"{entity_name}_extracted" for entity_name in ENTITY_DEFINITIONS.keys()]
            preview_cols = ['标题'] + extracted_column_names

            # 确保所有预览列都存在于DataFrame中，避免KeyError
            actual_preview_cols = [col for col in preview_cols if col in updated_df.columns]
            if not actual_preview_cols:
                print("没有可供预览的列（标题或提取列不存在于DataFrame中）。")
            else:
                print(updated_df[actual_preview_cols].head())

        except Exception as e:
            print(f"保存结果到CSV或预览时发生错误: {e}")
    else:
        print("未能处理数据，未生成输出文件。")