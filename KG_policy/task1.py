import pandas as pd
from neo4j import GraphDatabase, basic_auth
import re
from datetime import datetime

# --- 1. Neo4j 连接配置 ---
URI = "neo4j://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "88888888"  # 请替换为您的实际密码

# --- 2. 阈值配置 (调整后，增加通用阈值) ---
THRESHOLDS_CONFIG = {
    "__GENERAL__": { # 特殊键，用于不指定主题和受益人时的通用评估
        "__ANY__": {   # 特殊键，用于通用评估下的任何情况
            "min_policies": 1,                          # 至少有1条政策
            "max_avg_policy_age_days": 365 * 7,         # 平均政策年龄不超过7年 (非常宽松)
            "latest_policy_min_recency_days": 365 * 5,  # 最新政策距今不超过5年 (非常宽松)
            "required_levels_any": [],                  # 无特定层级要求
            "min_distinct_tools": 1,                    # 至少有1种政策工具
            "min_quantitative_details_count": 0         # 对量化信息无硬性要求
        }
    },
    "融资支持": { # 保留之前的特定主题阈值作为示例，但本次分析不用它
        "中小微企业": {
            "min_policies": 1,
            "max_avg_policy_age_days": 365 * 5,
            "latest_policy_min_recency_days": 365 * 3,
            "required_levels_any": [],
            "min_distinct_tools": 1,
            "min_quantitative_details_count": 0
        },
        "科技型民营企业": {
            "min_policies": 1,
            "max_avg_policy_age_days": 365 * 4,
            "latest_policy_min_recency_days": 365 * 2,
            "required_levels_any": [],
            "min_distinct_tools": 1,
            "min_quantitative_details_count": 0
        }
    },
    "减税降费": { # 保留之前的特定主题阈值作为示例
        "各类民营企业": {
            "min_policies": 1,
            "max_avg_policy_age_days": 365 * 5,
            "latest_policy_min_recency_days": 365 * 3,
            "min_distinct_tools": 1,
            "min_quantitative_details_count": 0
        }
    }
}

# --- 3. Python 端的数据处理与分析函数 ---

# parse_quantitative_info_components 函数 (保持不变)
def parse_quantitative_info_components(quantitative_detail_str):
    if not quantitative_detail_str or pd.isna(quantitative_detail_str) or not isinstance(quantitative_detail_str, str):
        return []
    match = re.search(r'\(([^)]*)\)$', quantitative_detail_str)
    if match:
        components_str = match.group(1)
        return [comp.strip() for comp in components_str.split(', ') if comp.strip()]
    return []

# get_policy_metrics_for_scope 函数 (修改后，处理可选的 topic_name 和 beneficiary_name_pattern)
def get_policy_metrics_for_scope(tx, topic_name, beneficiary_name_pattern, region_name):
    """
    从Neo4j查询指定范围内的政策指标。
    如果 topic_name 或 beneficiary_name_pattern 为 None 或空字符串，则不应用相应筛选。
    """
    # 构建动态的WHERE子句条件
    topic_condition = "($topic_name IS NULL OR $topic_name = '' OR EXISTS((p)-[:HAS_TOPIC]->(:PolicyTopic {name: $topic_name})))"
    beneficiary_condition = """
    ($beneficiary_name_pattern IS NULL OR $beneficiary_name_pattern = '' OR EXISTS {
        MATCH (p)-[:TARGETS_BENEFICIARY]->(tb_check:TargetBeneficiary)
        WHERE tb_check.name CONTAINS $beneficiary_name_pattern
    })
    """

    query_robust = f"""
    MATCH (geo:GeographicRegion {{name: $region_name}})
    WITH geo
    OPTIONAL MATCH (p:Policy)-[:APPLICABLE_IN]->(geo)
    WHERE p.validationStatus = '现行有效'
      AND {topic_condition}
      AND {beneficiary_condition}
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
    result = tx.run(query_robust,
                    topic_name=topic_name,
                    beneficiary_name_pattern=beneficiary_name_pattern,
                    region_name=region_name)
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
    min_recency = specific_thresholds.get("latest_policy_min_recency_days", float('inf'))
    if latest_policy_date_obj:
        latest_policy_py_date = datetime(latest_policy_date_obj.year, latest_policy_date_obj.month, latest_policy_date_obj.day)
        days_since_latest = (datetime.now() - latest_policy_py_date).days
        if days_since_latest > min_recency:
             weaknesses.append(f"最新政策发布距今过久: {days_since_latest}天 (阈值不超过 {min_recency}天)")
    elif num_policies >= min_p :
        weaknesses.append(f"无法确定最新政策时效性 (阈值要求最新政策距今不超过 {min_recency}天)")

    policy_levels_present = set(metrics.get("policyLevels", []))
    required_levels_any = set(specific_thresholds.get("required_levels_any", []))
    if required_levels_any and not (policy_levels_present & required_levels_any):
        weaknesses.append(f"缺少关键政策层级中的任何一种: {', '.join(required_levels_any)} (现有: {', '.join(policy_levels_present) if policy_levels_present else '无'})")

    num_distinct_tools = metrics.get("numberOfDistinctTools", 0)
    min_tools = specific_thresholds.get("min_distinct_tools", 0)
    if num_distinct_tools < min_tools:
        weaknesses.append(f"政策工具种类不足: {num_distinct_tools} (阈值至少 {min_tools})")

    valid_quantitative_details = metrics.get("quantitativeDetails", [])
    min_quant_details = specific_thresholds.get("min_quantitative_details_count", 0)
    if len(valid_quantitative_details) < min_quant_details:
        weaknesses.append(f"包含具体量化支持的政策不足: {len(valid_quantitative_details)} (阈值至少 {min_quant_details})")

    return weaknesses if weaknesses else ["当前观察维度下，政策支持度相对满足设定的低阈值要求。"]


# --- 4. 主执行逻辑 ---
def main_analysis():
    driver = None
    try:
        driver = GraphDatabase.driver(URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        print("成功连接到 Neo4j 数据库!")
    except Exception as e:
        print(f"连接 Neo4j 失败: {e}")
        return

    # --- B. 定义分析范围 (不再限定主题和受益人) ---
    target_topic = None  # 或者 "" 表示不筛选主题
    target_beneficiary_pattern = None  # 或者 "" 表示不筛选受益人
    target_region_name = "北京市"
    national_baseline_region_name = "全国各省、自治区、直辖市、新疆生产建设兵团"

    scope_description = f"区域='{target_region_name}' (所有主题, 所有受益人)"
    print(f"\n--- 开始分析: {scope_description} ---")

    with driver.session(database="neo4j") as session:
        # --- C. 查询目标区域的指标 ---
        print(f"\n正在查询 '{target_region_name}' 的总体政策指标...")
        target_region_metrics = session.execute_read(
            get_policy_metrics_for_scope,
            target_topic,
            target_beneficiary_pattern,
            target_region_name
        )

        print("\n目标区域指标:")
        if target_region_metrics:
            for key, value in target_region_metrics.items():
                if key == "averagePolicyAgeInDays" and value is not None:
                    print(f"  {key}: {value:.0f} 天 (~{(value/365.25):.1f} 年)")
                elif key == "latestPolicyAnnounceDate" and value is not None:
                     print(f"  {key}: {value.year}-{value.month:02d}-{value.day:02d}")
                else:
                    print(f"  {key}: {value}")
        else:
            print("  未能获取目标区域指标。")

        # --- D. 查询全国基准的指标 (同样不限定主题和受益人) ---
        print(f"\n正在查询 '{national_baseline_region_name}' (全国基准) 的总体政策指标...")
        national_baseline_metrics = session.execute_read(
            get_policy_metrics_for_scope,
            target_topic, # None
            target_beneficiary_pattern, # None
            national_baseline_region_name
        )

        print("\n全国基准指标:")
        if national_baseline_metrics:
            for key, value in national_baseline_metrics.items():
                if key == "averagePolicyAgeInDays" and value is not None:
                    print(f"  {key}: {value:.0f} 天 (~{(value/365.25):.1f} 年)")
                elif key == "latestPolicyAnnounceDate" and value is not None:
                     print(f"  {key}: {value.year}-{value.month:02d}-{value.day:02d}")
                else:
                    print(f"  {key}: {value}")
        else:
            print("  未能获取全国基准指标。")

        # --- E. 基于阈值评估目标区域的政策强度 (使用通用阈值) ---
        # 获取通用阈值配置
        thresholds_for_scope = THRESHOLDS_CONFIG.get("__GENERAL__", {}).get("__ANY__", None)

        print(f"\n对 '{target_region_name}' 进行基于通用低阈值的薄弱点评估:")
        if not target_region_metrics or target_region_metrics.get("numberOfPolicies") is None:
             print("  目标区域数据缺失或无相关政策，无法进行评估。")
        elif not thresholds_for_scope:
            print("  未找到通用的阈值配置 (THRESHOLDS_CONFIG['__GENERAL__']['__ANY__'])。")
        else:
            weaknesses = assess_policy_strength(target_region_metrics, thresholds_for_scope)
            if isinstance(weaknesses, list) and len(weaknesses) > 0:
                if weaknesses[0] != "当前观察维度下，政策支持度相对满足设定的低阈值要求。":
                    for item in weaknesses:
                        print(f"  - 薄弱点: {item}")
                else:
                    print(f"  {weaknesses[0]}")
            elif isinstance(weaknesses, str):
                 print(f"  {weaknesses}")
            else:
                print("  评估完成，未识别到特定薄弱点或评估信息不足。")

    if driver:
        driver.close()
    print("\n--- 分析完成 ---")

if __name__ == "__main__":
    main_analysis()