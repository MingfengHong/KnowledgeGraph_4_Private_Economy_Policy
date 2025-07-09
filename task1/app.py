from flask import Flask, request, jsonify
from flask_cors import CORS  # 用于处理跨域请求
from neo4j import GraphDatabase, basic_auth
from neo4j.time import Date, DateTime, Time, Duration  # 用于类型检查和转换
import collections.abc  # 用于更准确地检查字典和列表类型
import re
from datetime import datetime
import pandas as pd  # 保留，因为前端的某些辅助函数可能仍在使用

# --- 1. Neo4j 连接配置 ---
URI = "neo4j://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "88888888"  # 请务必替换为您的实际密码

app = Flask(__name__)
CORS(app)  # 允许所有来源的跨域请求 (生产环境中应配置得更严格)


# --- Neo4j驱动程序实例辅助函数 ---
def get_db_driver():
    # 每次请求都创建新的驱动实例不是最高效的，
    # 在生产环境中，通常在应用启动时创建一次。
    # 但对于此示例，这样可以工作。
    return GraphDatabase.driver(URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))


# --- 2. 数据处理与分析函数 ---

def convert_neo_to_serializable(item):
    """
    递归地将字典或列表中的Neo4j时间类型对象转换为JSON可序列化的字符串或结构。
    """
    if isinstance(item, collections.abc.Mapping):
        return {k: convert_neo_to_serializable(v) for k, v in item.items()}
    elif isinstance(item, collections.abc.Sequence) and not isinstance(item, (str, bytes)):
        return [convert_neo_to_serializable(x) for x in item]
    elif isinstance(item, Date) or isinstance(item, DateTime) or isinstance(item, Time):
        return item.iso_format()
    elif isinstance(item, Duration):
        return {"days": item.days, "seconds": item.seconds, "nanoseconds": item.nanoseconds}
    return item


# get_policy_metrics_for_scope 函数 (修改后，处理可选的筛选条件)
def get_policy_metrics_for_scope(tx, topic_name, target_beneficiary_name, region_name, policy_tool_category):
    """
    从Neo4j查询指定范围内的政策指标。
    如果 topic_name, target_beneficiary_name, 或 policy_tool_category 为 None 或空字符串，则不应用相应筛选。
    """
    params = {"region_name": region_name}

    # 基础的WHERE条件
    base_conditions = ["p.validationStatus = '现行有效'"]

    if topic_name and topic_name.strip():
        base_conditions.append("EXISTS((p)-[:HAS_TOPIC]->(:PolicyTopic {name: $topic_name_param}))")
        params["topic_name_param"] = topic_name.strip()

    if target_beneficiary_name and target_beneficiary_name.strip():
        # 假设下拉框提供的是精确的受益人名称，使用精确匹配
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

    query_robust = f"""
    MATCH (geo:GeographicRegion {{name: $region_name}})
    WITH geo
    OPTIONAL MATCH (p:Policy)-[:APPLICABLE_IN]->(geo)
    WHERE {where_clause} // 应用动态构建的WHERE子句
    WITH geo, collect(DISTINCT p) AS policies_in_scope

    UNWIND (CASE
              WHEN size(policies_in_scope) = 0 THEN [null]
              ELSE policies_in_scope
            END) AS p_scoped

    OPTIONAL MATCH (p_scoped)-[r_tool:APPLIES_TOOL]->(tool:PolicyTool)

    RETURN
        geo.name AS regionName,
        size(policies_in_scope) AS numberOfPolicies,
        max(p_scoped.announceDate) AS latestPolicyAnnounceDate,
        avg(duration.between(p_scoped.implementDate, date()).days) AS averagePolicyAgeInDays,
        [lvl IN collect(DISTINCT p_scoped.policyLevel) WHERE lvl IS NOT NULL] AS policyLevels,
        count(DISTINCT tool.name) AS numberOfDistinctTools,
        [cat IN collect(DISTINCT tool.category) WHERE cat IS NOT NULL] AS toolCategories,
        [qd IN collect(DISTINCT r_tool.quantitativeDetail) WHERE qd IS NOT NULL] AS quantitativeDetails
    """
    result = tx.run(query_robust, **params)  # 传递所有构建好的参数
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


# assess_policy_strength 函数 (保持不变)
def assess_policy_strength(metrics, specific_thresholds):
    weaknesses = []
    if not metrics or metrics.get("numberOfPolicies") is None:
        return ["数据缺失或查询范围内无任何政策"]

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
    if latest_policy_date_obj:  # Neo4j Date object
        latest_policy_py_date = datetime(latest_policy_date_obj.year, latest_policy_date_obj.month,
                                         latest_policy_date_obj.day)
        days_since_latest = (datetime.now() - latest_policy_py_date).days
        if days_since_latest > max_recency_days:
            weaknesses.append(f"最新政策发布距今过久: {days_since_latest}天 (阈值不超过 {max_recency_days}天)")
    elif num_policies >= min_p:
        weaknesses.append(f"无法确定最新政策时效性 (阈值要求最新政策距今不超过 {max_recency_days}天)")

    policy_levels_present = set(metrics.get("policyLevels", []))
    required_levels_any = set(specific_thresholds.get("required_levels_any", []))  # 这是从前端传入的数组
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


# --- Flask API 端点 ---
@app.route('/api/analyze_policy_strength', methods=['POST'])
def analyze_policy_strength_api():
    data = request.json

    region_name = data.get('region_name')
    policy_topic = data.get('policy_topic')
    target_beneficiary_name = data.get('target_beneficiary_name')  # 从前端获取
    policy_tool_category = data.get('policy_tool_category')  # 从前端获取
    user_thresholds = data.get('user_thresholds')

    if not region_name or user_thresholds is None:
        return jsonify({"error": "区域名称和阈值配置是必填项"}), 400

    driver = None
    try:
        driver = get_db_driver()
        # driver.verify_connectivity() # 在get_db_driver中通常不做，或者只做一次

        national_baseline_region_name = "全国各省、自治区、直辖市、新疆生产建设兵团"

        with driver.session(database="neo4j") as session:
            target_metrics_raw = session.execute_read(
                get_policy_metrics_for_scope,
                policy_topic,
                target_beneficiary_name,
                region_name,
                policy_tool_category
            )
            national_metrics_raw = session.execute_read(
                get_policy_metrics_for_scope,
                policy_topic,
                target_beneficiary_name,
                national_baseline_region_name,
                policy_tool_category
            )

            weaknesses_assessment_result = []
            if target_metrics_raw:
                weaknesses_assessment_result = assess_policy_strength(target_metrics_raw, user_thresholds)
            else:
                # 默认结构以防万一，尽管 get_policy_metrics_for_scope 应该总是返回一个字典
                default_metrics_structure = {
                    "regionName": region_name, "numberOfPolicies": 0,
                    "latestPolicyAnnounceDate": None, "averagePolicyAgeInDays": None,
                    "policyLevels": [], "numberOfDistinctTools": 0,
                    "toolCategories": [], "quantitativeDetails": []
                }
                weaknesses_assessment_result = assess_policy_strength(default_metrics_structure, user_thresholds)

            # 在 jsonify 之前，将 Neo4j Date/Time 对象转换为字符串
            target_metrics_serializable = convert_neo_to_serializable(target_metrics_raw)
            national_metrics_serializable = convert_neo_to_serializable(national_metrics_raw)

            return jsonify({
                "target_region_metrics": target_metrics_serializable,
                "national_baseline_metrics": national_metrics_serializable,
                "assessment_weaknesses": weaknesses_assessment_result
            })
    except Exception as e:
        print(f"API Error: {e}")
        # 在开发阶段可以返回详细错误，生产环境应记录并返回通用错误
        import traceback
        traceback.print_exc()
        return jsonify({"error": "服务器处理请求时发生错误: " + str(e)}), 500
    finally:
        if driver:
            driver.close()


if __name__ == '__main__':
    print("Flask应用准备启动，请确保Neo4j数据库正在运行并且连接配置正确。")
    print(f"将在 http://127.0.0.1:5001/ 上运行。")
    print("前端HTML文件 (index.html) 需要能够访问此后端API。")
    app.run(debug=True, port=5001)