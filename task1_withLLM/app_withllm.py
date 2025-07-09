from flask import Flask, request, jsonify
from flask_cors import CORS
from neo4j import GraphDatabase, basic_auth
from neo4j.time import Date, DateTime, Time, Duration  # 确保导入 Neo4j 时间类型
import collections.abc  # 使用 collections.abc.Mapping 和 collections.abc.Sequence
import re
from datetime import datetime
# pandas is not actively used in the core logic of app_with_llm.py for query or LLM interaction
# import pandas as pd
import json

# --- 0. LLM Configuration (DeepSeek) ---
from openai import OpenAI

DEEPSEEK_API_KEY = "sk-xxx"  # 请替换为您的真实API Key

llm_client = None
if DEEPSEEK_API_KEY and DEEPSEEK_API_KEY != "sk-YOUR_DEEPSEEK_API_KEY":
    try:
        llm_client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com")
        print("DeepSeek LLM客户端已初始化。")
    except Exception as e:
        print(f"初始化DeepSeek LLM客户端失败: {e}")
else:
    print("警告: DeepSeek API Key 未设置或使用的是占位符。LLM功能将不可用。")

# --- 1. Neo4j 连接配置 ---
URI = "neo4j://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "88888888"

app = Flask(__name__)
CORS(app)


# --- Neo4j驱动程序实例辅助函数 ---
def get_db_driver():
    return GraphDatabase.driver(URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))


# --- 2. 数据处理与分析函数 ---
def convert_neo_to_serializable(item):
    if isinstance(item, collections.abc.Mapping):  # 使用 collections.abc.Mapping
        return {k: convert_neo_to_serializable(v) for k, v in item.items()}
    elif isinstance(item, collections.abc.Sequence) and not isinstance(item,
                                                                       (str, bytes)):  # 使用 collections.abc.Sequence
        return [convert_neo_to_serializable(x) for x in item]
    elif isinstance(item, (Date, DateTime, Time)):  # Neo4j 时间类型
        return item.iso_format()
    elif isinstance(item, Duration):  # Neo4j Duration 类型
        # 将Duration转换为更易读的格式或总秒数等
        return {"days": item.days, "seconds": item.seconds, "nanoseconds": item.nanoseconds,
                "total_seconds": item.seconds + item.days * 86400 + item.nanoseconds / 1e9}
    return item


def get_policy_metrics_for_scope(tx, topic_name, target_beneficiary_name, region_name, policy_tool_category):
    params = {"region_name": region_name}
    base_conditions = ["p.validationStatus = '现行有效'"]  # 'p' will be the alias for policy nodes

    if topic_name and topic_name.strip():
        base_conditions.append("EXISTS((p)-[:HAS_TOPIC]->(:PolicyTopic {name: $topic_name_param}))")
        params["topic_name_param"] = topic_name.strip()
    if target_beneficiary_name and target_beneficiary_name.strip():
        base_conditions.append(
            "EXISTS((p)-[:TARGETS_BENEFICIARY]->(:TargetBeneficiary {name: $target_beneficiary_name_param}))")
        params["target_beneficiary_name_param"] = target_beneficiary_name.strip()
    if policy_tool_category and policy_tool_category.strip():
        base_conditions.append("""EXISTS {
            MATCH (p)-[:APPLIES_TOOL]->(pt_check:PolicyTool)
            WHERE pt_check.category = $policy_tool_category_param
        }""")
        params["policy_tool_category_param"] = policy_tool_category.strip()

    where_clause = " AND ".join(base_conditions)
    if not where_clause.strip():
        where_clause = "true"

    national_baseline_region_name_literal = "全国各省、自治区、直辖市、新疆生产建设兵团"

    query_robust = f"""
    MATCH (target_geo:GeographicRegion {{name: $region_name}})
    OPTIONAL MATCH (national_geo:GeographicRegion {{name: "{national_baseline_region_name_literal}"}})

    CALL {{
        WITH target_geo, national_geo

        OPTIONAL MATCH (target_geo)-[:IS_SUBREGION_OF]->(parent_prov_geo:GeographicRegion)

        OPTIONAL MATCH (p_direct:Policy)-[:APPLICABLE_IN]->(target_geo)
        WITH target_geo, national_geo, parent_prov_geo, collect(DISTINCT p_direct) AS direct_policies_list

        OPTIONAL MATCH (p_provincial:Policy)-[:APPLICABLE_IN]->(parent_prov_geo)
        WITH target_geo, national_geo, parent_prov_geo, direct_policies_list,
             (CASE 
                WHEN parent_prov_geo IS NOT NULL 
                THEN collect(DISTINCT p_provincial) 
                ELSE [] 
              END) AS provincial_policies_list

        OPTIONAL MATCH (p_national:Policy)-[:APPLICABLE_IN]->(national_geo)
        WITH target_geo, national_geo, parent_prov_geo, direct_policies_list, provincial_policies_list,
             collect(DISTINCT p_national) AS national_policies_list

        // Determine policy list based on target_geo's level and name
        // Assumes target_geo.level: 1 for Province, 2 for City (as integers)
        WITH direct_policies_list, provincial_policies_list, national_policies_list, target_geo, parent_prov_geo
        WITH CASE
                // If target is National
                WHEN target_geo.name = "{national_baseline_region_name_literal}" THEN national_policies_list 
                // If target is Provincial (level 1)
                WHEN target_geo.level = 1 THEN direct_policies_list 
                // If target is City (level 2)
                WHEN target_geo.level = 2 AND parent_prov_geo IS NOT NULL THEN direct_policies_list + provincial_policies_list
                WHEN target_geo.level = 2 AND parent_prov_geo IS NULL THEN direct_policies_list // City without a parent in KG, treat as province
                // Default for other levels or if level is missing: only direct policies for that specific region
                ELSE direct_policies_list 
             END AS all_candidate_policies_with_duplicates

        UNWIND all_candidate_policies_with_duplicates AS p_candidate_unwound 
        WITH collect(DISTINCT p_candidate_unwound) AS unique_candidate_policies 

        UNWIND (CASE WHEN size(unique_candidate_policies) = 0 THEN [null] ELSE unique_candidate_policies END) AS p_unwound_for_filter

        WITH p_unwound_for_filter AS p 
        WHERE p IS NOT NULL AND ({where_clause})

        WITH collect(DISTINCT p) AS collected_filtered_policies
        RETURN collected_filtered_policies AS filtered_policies 
    }}

    WITH target_geo, filtered_policies AS policies_in_scope
    UNWIND (CASE WHEN size(policies_in_scope) = 0 THEN [null] ELSE policies_in_scope END) AS p_scoped 

    OPTIONAL MATCH (p_scoped)-[r_tool:APPLIES_TOOL]->(tool:PolicyTool)

    RETURN
        target_geo.name AS regionName,
        size(policies_in_scope) AS numberOfPolicies,
        max(p_scoped.announceDate) AS latestPolicyAnnounceDate,
        avg(CASE WHEN p_scoped.implementDate IS NOT NULL THEN duration.between(p_scoped.implementDate, date()).days ELSE null END) AS averagePolicyAgeInDays,
        [lvl IN collect(DISTINCT p_scoped.policyLevel) WHERE lvl IS NOT NULL AND lvl <> ''] AS policyLevels,
        count(DISTINCT tool.name) AS numberOfDistinctTools,
        [cat IN collect(DISTINCT tool.category) WHERE cat IS NOT NULL AND cat <> ''] AS toolCategories,
        [qd IN collect(DISTINCT r_tool.quantitativeDetail) WHERE qd IS NOT NULL AND qd <> ''] AS quantitativeDetails
    """
    result = tx.run(query_robust, **params)
    record = result.single()

    data = record.data() if record else {"regionName": region_name, "numberOfPolicies": 0}
    if data.get("numberOfPolicies", 0) == 0:
        data["latestPolicyAnnounceDate"] = None
        data["averagePolicyAgeInDays"] = None
        data["policyLevels"] = []
        data["numberOfDistinctTools"] = 0
        data["toolCategories"] = []
        data["quantitativeDetails"] = []
    return data


def assess_policy_strength(metrics, specific_thresholds):
    weaknesses = []
    if not metrics or (metrics.get("numberOfPolicies", 0) == 0 and metrics.get("latestPolicyAnnounceDate") is None):
        return ["指定范围内未查询到有效政策，或政策指标数据不足。"]

    num_policies = metrics.get("numberOfPolicies", 0)
    min_p = specific_thresholds.get("min_policies", 0)
    if num_policies < min_p:
        weaknesses.append(f"政策数量不足: {num_policies} (阈值至少 {min_p})")

    avg_age_days = metrics.get("averagePolicyAgeInDays")
    max_avg_age = specific_thresholds.get("max_avg_policy_age_days", float('inf'))
    if avg_age_days is not None and avg_age_days > max_avg_age:
        age_years = avg_age_days / 365.25
        expected_age_years = max_avg_age / 365.25
        weaknesses.append(f"政策平均年龄过大: {age_years:.1f}年 (阈值不超过 {expected_age_years:.1f}年)")

    latest_policy_date_obj = metrics.get("latestPolicyAnnounceDate")
    max_recency_days = specific_thresholds.get("latest_policy_min_recency_days", float('inf'))
    if latest_policy_date_obj:
        latest_policy_py_date = None
        if isinstance(latest_policy_date_obj, str):
            try:
                latest_policy_py_date = datetime.fromisoformat(latest_policy_date_obj.split('T')[0])
            except ValueError:
                pass
        elif hasattr(latest_policy_date_obj, 'year'):
            latest_policy_py_date = datetime(latest_policy_date_obj.year, latest_policy_date_obj.month,
                                             latest_policy_date_obj.day)

        if latest_policy_py_date:
            days_since_latest = (datetime.now() - latest_policy_py_date).days
            if days_since_latest > max_recency_days:
                weaknesses.append(f"最新政策发布距今过久: {days_since_latest}天 (阈值不超过 {max_recency_days}天)")
        elif num_policies >= min_p:
            weaknesses.append(f"无法确定最新政策时效性 (阈值要求最新政策距今不超过 {max_recency_days}天)")
    elif num_policies >= min_p:
        weaknesses.append(f"无法确定最新政策时效性 (阈值要求最新政策距今不超过 {max_recency_days}天)")

    policy_levels_present = set(metrics.get("policyLevels", []))
    required_levels_any = set(specific_thresholds.get("required_levels_any", []))
    if required_levels_any and not (policy_levels_present & required_levels_any):
        weaknesses.append(
            f"缺少关键政策层级中的任何一种: {', '.join(required_levels_any)} (现有: {', '.join(policy_levels_present) if policy_levels_present else '无'})")

    num_distinct_tools = metrics.get("numberOfDistinctTools", 0)
    min_tools = specific_thresholds.get("min_distinct_tools", 0)
    if num_distinct_tools < min_tools:
        weaknesses.append(f"政策工具种类不足: {num_distinct_tools} (阈值至少 {min_tools})")

    valid_quantitative_details = metrics.get("quantitativeDetails", [])
    min_quant_details = specific_thresholds.get("min_quantitative_details_count", 0)
    if len(valid_quantitative_details) < min_quant_details:
        weaknesses.append(
            f"包含具体量化支持的政策不足: {len(valid_quantitative_details)} (阈值至少 {min_quant_details})")

    return weaknesses if weaknesses else ["当前观察维度下，政策支持度相对满足用户设定的阈值要求。"]


# --- LLM 调用函数 ---
def call_deepseek_llm_for_analysis(target_metrics_str, national_metrics_str, rule_based_weaknesses_str,
                                   user_query_context_str, user_thresholds_str, target_region_name):
    global llm_client
    if not llm_client:
        print("LLM客户端未初始化。将返回预设错误信息。")
        return {
            "llm_target_metrics_summary": "LLM服务未配置或初始化失败：无法生成目标区域指标分析。",
            "llm_national_metrics_summary": "LLM服务未配置或初始化失败：无法生成全国基准指标参考。",
            "llm_weakness_assessment": "LLM服务未配置或初始化失败：无法生成详细薄弱点评估报告。"
        }

    prompt_content = f"""
    作为一名资深的中国民营经济政策分析专家，请根据以下提供的原始数据和分析要求，生成一份关于“{target_region_name}”在“{user_query_context_str}”方面的民营经济政策支持情况分析报告。
    请严格按照下面三个部分组织您的回答，并针对每个部分采用清晰、专业且用户友好的语言风格，对 quantitativeDetails 等列表信息进行恰当的解读、分类和摘要，数值较大的金额请使用如“万亿元”这样的易读单位。请直接输出报告内容，不要包含额外的对话或解释。

    ---
    ### 第一部分：{target_region_name}政策指标分析

    **请分析以下“{target_region_name}”的原始指标数据：**
    ```json
    {target_metrics_str}
    ```
    **要求：**
    1. 将英文或内部键名（如 averagePolicyAgeInDays, quantitativeDetails）转换为流畅的中文描述（例如：“平均政策生效时间”, “关键量化信息摘要”）。
    2. 对数据进行结构化展示，可以参考类似如下的格式：
        * **区域名称：** {target_region_name}
        * **政策统计：**
            * 政策数量：[根据数据填写] 项
            * 平均政策生效时间：[根据数据计算并填写，例如 X 天 (约 Y 年)]
            * 最新政策发布日期：[根据数据填写]
        * **政策特征：**
            * 政策层级：[根据数据罗列，例如 地方规范性文件, 省级...]
            * 政策工具种类数量：[根据数据填写] 种
            * 主要政策工具类别：[根据数据罗列]
        * **关键量化信息摘要：** (请从此部分数据中智能提取和总结，例如：)
            * [示例] 涉及总金额：约 X 亿元
            * [示例] 单项补贴金额上限示例：Y 万元
            * [示例] 其他显著量化信息点...
    ---
    ### 第二部分：全国政策基准指标参考

    **请分析以下“全国基准”的原始指标数据：**
    ```json
    {national_metrics_str}
    ```
    **要求：**
    1. 与第一部分类似，将键名转换为中文，并结构化展示。
    2. 对 'quantitativeDetails' 进行智能的分类、提炼和摘要，使用“如”、“例如”等词语表明是示例性数据。
    3. 参考格式：
        * **覆盖范围：** 全国各省、自治区、直辖市及新疆生产建设兵团
        * **政策统计均值/概况：**
            * 政策数量（样本）：[根据数据填写] 项
            * 平均政策生效时间：[根据数据计算并填写]
            * 最新政策发布日期（样本）：[根据数据填写]
        * **政策特征均值/概况：**
            * 主要政策层级：[根据数据罗列]
            * 政策工具种类数量（综合）：[根据数据填写] 种
            * 主要政策工具类别：[根据数据罗列]
        * **代表性量化信息示例：** (请从此部分数据中智能提取和总结)
            * [示例] 企业所得税优惠税率：如 X%
            * [示例] 财政支持/减负总额（估算/汇总）：约 Y 万亿元
            * [示例] 其他显著量化信息点...
    ---
    ### 第三部分：{target_region_name}详细薄弱点评估报告

    **请结合以下所有信息：**
    1. “{target_region_name}”的原始指标 (见第一部分)。
    2. “全国基准”的原始指标 (见第二部分)。
    3. 用户为“{target_region_name}”设定的评估阈值：
        ```json
        {user_thresholds_str}
        ```
    4. 系统基于上述阈值初步识别的薄弱点：
        {rule_based_weaknesses_str}

    **要求：**
    生成一份详细、专业的薄弱点评估报告。报告应：
    * 明确指出与用户设定阈值相比，目标区域存在的具体政策支持不足之处。
    * 与全国基准进行对比，分析目标区域的相对优势或劣势。注意：地区的量化指标不能与全国的直接比较，例如某类政策全国支持的资金池为10万亿级别，则不能要求地方也有相同数量级的支持。重点考虑地方公布的量化指标是否与其经济体量相匹配。
    * 对“薄弱”的原因进行有逻辑的推测或解释。
    * 如果系统初步识别的薄弱点为空或提示“相对满足”，请确认政策覆盖情况是否良好，或指出即使满足基本阈值，与全国基准相比可能存在的差距或值得进一步关注的方面。
    * 语言专业、客观，并给出建设性的总结。
    ---
    请确保三部分内容完整且格式清晰，每一部分都以对应的 "### 第一部分：..." "### 第二部分：..." "### 第三部分：..." 作为开头。
    """
    try:
        print("\n[LLM] 正在调用DeepSeek LLM API进行分析和报告生成...")
        response = llm_client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system",
                 "content": "你是一位资深的中国民营经济政策分析专家，擅长解读政策数据并撰写专业的、结构清晰的分析报告。请严格按照用户要求的格式和风格输出。"},
                {"role": "user", "content": prompt_content}
            ],
            stream=False,
            temperature=0.2,
            max_tokens=8192
        )

        llm_full_response = response.choices[0].message.content
        print("[LLM] LLM API调用成功。正在解析响应...")

        report_parts = {
            "llm_target_metrics_summary": "未能从LLM响应中解析目标区域指标总结。",
            "llm_national_metrics_summary": "未能从LLM响应中解析全国基准指标总结。",
            "llm_weakness_assessment": "未能从LLM响应中解析薄弱点评估报告。"
        }

        part1_marker = f"### 第一部分：{target_region_name}政策指标分析"
        part2_marker = "### 第二部分：全国政策基准指标参考"
        part3_marker = f"### 第三部分：{target_region_name}详细薄弱点评估报告"

        idx1 = llm_full_response.find(part1_marker)
        idx2 = llm_full_response.find(part2_marker)
        idx3 = llm_full_response.find(part3_marker)

        if idx1 != -1:
            start1 = idx1 + len(part1_marker)
            end1 = idx2 if idx2 != -1 else (idx3 if idx3 != -1 else len(llm_full_response))
            report_parts["llm_target_metrics_summary"] = llm_full_response[start1:end1].strip()

        if idx2 != -1:
            start2 = idx2 + len(part2_marker)
            end2 = idx3 if idx3 != -1 else len(llm_full_response)
            report_parts["llm_national_metrics_summary"] = llm_full_response[start2:end2].strip()

        if idx3 != -1:
            start3 = idx3 + len(part3_marker)
            report_parts["llm_weakness_assessment"] = llm_full_response[start3:].strip()

        if all(val.startswith("未能从LLM响应中解析") for val in report_parts.values()) and llm_full_response.strip():
            print("[LLM] 警告：未能按预期分割LLM响应，将整个响应放入评估报告中。")
            report_parts["llm_weakness_assessment"] = "LLM完整响应：\n" + llm_full_response

        print("[LLM] LLM响应解析尝试完成。")
        return report_parts

    except Exception as e:
        print(f"调用LLM时发生错误: {e}")
        error_msg = f"处理LLM请求时发生错误: {str(e)}"
        return {
            "llm_target_metrics_summary": error_msg,
            "llm_national_metrics_summary": error_msg,
            "llm_weakness_assessment": error_msg
        }


# --- Flask API 端点 ---
@app.route('/api/analyze_policy_strength_with_llm', methods=['POST'])
def analyze_policy_strength_with_llm_api():
    data = request.json
    region_name = data.get('region_name')
    policy_topic = data.get('policy_topic')
    target_beneficiary_name = data.get('target_beneficiary_name')
    policy_tool_category = data.get('policy_tool_category')
    user_thresholds = data.get('user_thresholds')

    if not region_name or not isinstance(user_thresholds, dict):
        return jsonify({"error": "区域名称和有效的阈值配置是必填项"}), 400

    driver = None
    try:
        driver = get_db_driver()
        national_baseline_region_name = "全国各省、自治区、直辖市、新疆生产建设兵团"

        raw_target_metrics = None
        raw_national_metrics = None
        rule_based_weaknesses = ["未能获取目标区域指标，无法进行规则评估。"]

        with driver.session(database="neo4j") as session:
            raw_target_metrics = session.execute_read(
                get_policy_metrics_for_scope,
                policy_topic or None,
                target_beneficiary_name or None,
                region_name,
                policy_tool_category or None
            )
            # 全国基准的指标仍然按照原来的逻辑获取（包含全国政策）
            # 如果需要调整全国范围的逻辑，需要修改此处的调用或get_policy_metrics_for_scope内部逻辑
            raw_national_metrics = session.execute_read(
                get_policy_metrics_for_scope,  # This call will now use the new logic.
                # If national_baseline_region_name is "全国...", it will get national policies.
                # If national_baseline_region_name was e.g. "江苏省", it would only get Jiangsu policies.
                # The current name "全国各省..." means it hits the national case correctly.
                policy_topic or None,
                target_beneficiary_name or None,
                national_baseline_region_name,
                policy_tool_category or None
            )

        if raw_target_metrics:
            rule_based_weaknesses = assess_policy_strength(raw_target_metrics, user_thresholds)

        target_metrics_serializable = convert_neo_to_serializable(raw_target_metrics)
        national_metrics_serializable = convert_neo_to_serializable(raw_national_metrics)

        target_metrics_str = json.dumps(target_metrics_serializable, ensure_ascii=False, indent=2)
        national_metrics_str = json.dumps(national_metrics_serializable, ensure_ascii=False, indent=2)
        user_thresholds_str = json.dumps(user_thresholds, ensure_ascii=False, indent=2)

        if not rule_based_weaknesses or rule_based_weaknesses[0] in [
            "当前观察维度下，政策支持度相对满足用户设定的阈值要求。", "指定范围内未查询到有效政策，或政策指标数据不足。"]:
            rule_based_weaknesses_str = "基于规则的初步评估显示：" + rule_based_weaknesses[0]
        else:
            rule_based_weaknesses_str = "基于规则的初步评估识别出以下潜在薄弱点：\n- " + "\n- ".join(
                rule_based_weaknesses)

        user_query_context_parts = [f"分析区域 '{region_name}'"]
        if policy_topic: user_query_context_parts.append(f"政策主题 '{policy_topic}'")
        if target_beneficiary_name: user_query_context_parts.append(f"主要受益对象 '{target_beneficiary_name}'")
        if policy_tool_category: user_query_context_parts.append(f"政策工具类别 '{policy_tool_category}'")
        user_query_context_str = "，".join(user_query_context_parts)
        if len(user_query_context_parts) == 1 and "分析区域" in user_query_context_parts[0]:
            user_query_context_str += " (所有相关主题、受益人和工具类别)"

        llm_generated_reports = call_deepseek_llm_for_analysis(
            target_metrics_str,
            national_metrics_str,
            rule_based_weaknesses_str,
            user_query_context_str,
            user_thresholds_str,
            region_name
        )

        return jsonify(llm_generated_reports)

    except Exception as e:
        print(f"API 主处理流程错误: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "服务器在主处理流程中发生错误: " + str(e)}), 500
    finally:
        if driver:
            driver.close()


if __name__ == '__main__':
    print("Flask应用准备启动 (带DeepSeek LLM分析功能)，请确保Neo4j数据库正在运行并且连接配置正确。")
    if llm_client:
        print("同时，请确保已配置有效的DEEPSEEK_API_KEY。")
    else:
        print("警告: DEEPSEEK_API_KEY 未配置或客户端初始化失败，LLM功能将受限。")
    print(f"将在 http://127.0.0.1:5001/ 上运行。")
    app.run(debug=True, port=5001)

