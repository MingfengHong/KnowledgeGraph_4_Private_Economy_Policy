import pandas as pd
from neo4j import GraphDatabase, basic_auth
import re  # For regular expressions

# --- 1. Neo4j 连接配置 ---
URI = "neo4j://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "88888888"  # 您提供的密码

# --- 2. 数据映射定义 ---
INDUSTRY_CODE_MAPPING = {
    "农、林、牧、渔业": "A",
    "采矿业": "B",
    "制造业": "C",
    "电力、热力、燃气及水生产和供应业": "D",
    "建筑业": "E",
    "批发和零售业": "F",
    "交通运输、仓储和邮政业": "G",
    "住宿和餐饮业": "H",
    "信息传输、软件和信息技术服务业": "I",
    "金融业": "J",
    "房地产业": "K",
    "租赁和商务服务业": "L",
    "科学研究和技术服务业": "M",
    "水利、环境和公共设施管理业": "N",
    "居民服务、修理和其他服务业": "O",
    "教育": "P",
    "卫生和社会工作": "Q",
    "文化、体育和娱乐业": "R",
    "公共管理、社会保障和社会组织": "S",
    "国际组织": "T",
    "全行业": "Z"
}


# --- 3. Python 端的数据处理函数 ---
def parse_quantitative_info_updated(quantitative_info_str):
    """
    解析 QuantitativeInfo 字符串。
    格式: "工具A(信息A); 工具B(信息B)"
    内部信息: "组件1, 组件2"
    返回: 字典 {"工具A": "信息A", "工具B": "信息B"}
    """
    if not quantitative_info_str or pd.isna(quantitative_info_str) or not isinstance(quantitative_info_str, str):
        return {}

    parsed_details = {}
    # 不同政策工具间用半角分号和空格（"; "）连接
    tools_info_parts = quantitative_info_str.strip().split('; ')

    for tool_info_part in tools_info_parts:
        if not tool_info_part.strip():
            continue
        # 政策工具名称(逗号分隔的组件信息)
        # 正则表达式匹配： 工具名称部分 和 括号内的细节部分
        match = re.match(r'^(.*?)\(([^)]*)\)$', tool_info_part.strip())
        if match:
            tool_name = match.group(1).strip()
            detail_string = match.group(2).strip()  # 这就是 "逗号分隔的组件信息"
            if tool_name:  # 确保工具名称不为空
                parsed_details[tool_name] = detail_string
    return parsed_details


def process_date_format(date_str):
    if date_str and isinstance(date_str, str) and re.match(r'^\d{4}\.\d{2}\.\d{2}$', date_str.strip()):
        return date_str.strip().replace('.', '-')
    elif date_str and isinstance(date_str, (int, float)) and not pd.isna(
            date_str):  # Handle potential excel date numbers
        try:
            return pd.to_datetime(date_str, unit='D', origin='1899-12-30').strftime('%Y-%m-%d')
        except:
            pass
    return None


# --- 4. Neo4j 事务函数 ---
def create_policy_and_issuer_tx(tx, policy_data):
    query = (
        "MERGE (p:Policy {fabaoCitation: $fabaoCitation}) "
        "ON CREATE SET "
        "    p.title = $title, p.documentNumber = $documentNumber, "
        "    p.announceDate = CASE WHEN $announceDate IS NOT NULL THEN date($announceDate) ELSE null END, "
        "    p.implementDate = CASE WHEN $implementDate IS NOT NULL THEN date($implementDate) ELSE null END, "
        "    p.policyLevel = $policyLevel, p.validationStatus = $validationStatus, p.fullText = $fullText "
        "MERGE (ib:IssuingBody {fullName: $issuingBodyFullName}) "
        "ON CREATE SET ib.shortName = $issuingBodyShortName "
        "MERGE (p)-[:ISSUED_BY]->(ib)"
    )
    tx.run(query, **policy_data)


def link_policy_to_simple_node_tx(tx, fabao_citation, node_label, relationship_type, node_name_field, node_name_value):
    if not node_name_value or (isinstance(node_name_value, float) and pd.isna(node_name_value)) or not str(
            node_name_value).strip():
        return
    node_name_value_clean = str(node_name_value).strip()
    query = (
        f"MATCH (p:Policy {{fabaoCitation: $fabao_citation}}) "
        f"MERGE (n:{node_label} {{{node_name_field}: $node_name_value}}) "
        f"MERGE (p)-[:{relationship_type}]->(n)"
    )
    tx.run(query, fabao_citation=fabao_citation, node_name_value=node_name_value_clean)


def link_policy_to_industry_focus_tx(tx, fabao_citation, industry_name, industry_code):
    if not industry_name or (isinstance(industry_name, float) and pd.isna(industry_name)) or not str(
            industry_name).strip():
        return
    industry_name_clean = str(industry_name).strip()

    params = {
        "fabao_citation": fabao_citation,
        "industry_name": industry_name_clean,
        "industry_code": industry_code
    }

    if industry_code:
        query = (
            "MATCH (p:Policy {fabaoCitation: $fabao_citation}) "
            "MERGE (indf:IndustryFocus {name: $industry_name}) "
            "SET indf.code = $industry_code "
            "MERGE (p)-[:FOCUSES_ON_INDUSTRY]->(indf)"
        )
    else:
        query = (
            "MATCH (p:Policy {fabaoCitation: $fabao_citation}) "
            "MERGE (indf:IndustryFocus {name: $industry_name}) "
            "MERGE (p)-[:FOCUSES_ON_INDUSTRY]->(indf)"
        )
    tx.run(query, **params)


def link_policy_to_tool_tx(tx, fabao_citation, tool_name, quantitative_detail_value):
    if not tool_name or (isinstance(tool_name, float) and pd.isna(tool_name)) or not str(tool_name).strip():
        return
    tool_name_clean = str(tool_name).strip()
    query = (
        "MATCH (p:Policy {fabaoCitation: $fabao_citation}) "
        "MERGE (ptool:PolicyTool {name: $tool_name}) "
        "MERGE (p)-[r:APPLIES_TOOL]->(ptool) "
        "SET r.quantitativeDetail = $quantitative_detail_value"
    )
    tx.run(query, fabao_citation=fabao_citation, tool_name=tool_name_clean,
           quantitative_detail_value=quantitative_detail_value)


def update_node_properties_tx(tx, node_label, node_identifier_field, node_identifier_value, properties_to_set):
    if not node_identifier_value or (isinstance(node_identifier_value, float) and pd.isna(node_identifier_value)):
        return
    node_identifier_value_clean = str(node_identifier_value).strip()

    set_clauses = []
    valid_params = {"node_identifier_value": node_identifier_value_clean}

    for key, value in properties_to_set.items():
        if value is not None and not (isinstance(value, float) and pd.isna(value)) and not pd.isna(value):
            set_clauses.append(f"n.{key} = ${key}")
            valid_params[key] = value

    if not set_clauses:
        return

    query = (
        f"MATCH (n:{node_label} {{{node_identifier_field}: $node_identifier_value}}) "
        f"SET {', '.join(set_clauses)}"
    )
    tx.run(query, **valid_params)


# --- 5. 数据库Schema设置函数 ---
def setup_database_schema(driver):
    """
    在 Neo4j 数据库中创建约束和索引。
    由于使用了 'IF NOT EXISTS'，这些操作是幂等的。
    """
    print("开始设置数据库Schema (约束和索引)...")
    with driver.session(database="neo4j") as session:
        try:
            # 约束 (同时会在唯一性属性上创建索引)
            constraints = [
                "CREATE CONSTRAINT policy_fabaoCitation_unique IF NOT EXISTS FOR (p:Policy) REQUIRE p.fabaoCitation IS UNIQUE;",
                "CREATE CONSTRAINT issuingbody_fullName_unique IF NOT EXISTS FOR (ib:IssuingBody) REQUIRE ib.fullName IS UNIQUE;",
                "CREATE CONSTRAINT policytopic_name_unique IF NOT EXISTS FOR (pt:PolicyTopic) REQUIRE pt.name IS UNIQUE;",
                "CREATE CONSTRAINT policytool_name_unique IF NOT EXISTS FOR (ptool:PolicyTool) REQUIRE ptool.name IS UNIQUE;",
                "CREATE CONSTRAINT targetbeneficiary_name_unique IF NOT EXISTS FOR (tb:TargetBeneficiary) REQUIRE tb.name IS UNIQUE;",
                "CREATE CONSTRAINT geographicregion_name_unique IF NOT EXISTS FOR (gr:GeographicRegion) REQUIRE gr.name IS UNIQUE;",
                "CREATE CONSTRAINT industryfocus_name_unique IF NOT EXISTS FOR (indf:IndustryFocus) REQUIRE indf.name IS UNIQUE;"
            ]
            for constraint_query in constraints:
                print(f"执行约束: {constraint_query}")
                session.run(constraint_query)
            print("所有约束已确保存在。")

            # 额外的索引 (用于非唯一但经常查询的属性)
            # Additional Indexes (for non-unique properties frequently used in lookups)
            indexes = [
                "CREATE INDEX policy_title_index IF NOT EXISTS FOR (p:Policy) ON (p.title);",
                "CREATE INDEX policytool_category_index IF NOT EXISTS FOR (ptool:PolicyTool) ON (ptool.category);",
                "CREATE INDEX geographicregion_code_index IF NOT EXISTS FOR (gr:GeographicRegion) ON (gr.code);",
                "CREATE INDEX geographicregion_level_index IF NOT EXISTS FOR (gr:GeographicRegion) ON (gr.level);",
                "CREATE INDEX industryfocus_code_index IF NOT EXISTS FOR (indf:IndustryFocus) ON (indf.code);"
            ]
            for index_query in indexes:
                print(f"执行索引: {index_query}")
                session.run(index_query)
            print("所有额外索引已确保存在。")

            print("数据库Schema设置完成。")
        except Exception as e:
            print(f"设置数据库Schema时出错: {e}")
            # 如果Schema设置失败，可能不应该继续加载数据，所以抛出异常
            raise


# --- 6. 主执行逻辑 ---
def main():
    with GraphDatabase.driver(URI, auth=basic_auth(NEO4J_USER, NEO4J_PASSWORD)) as driver:
        try:
            driver.verify_connectivity()
            print("成功连接到 Neo4j 数据库!")
        except Exception as e:
            print(f"连接失败: {e}")
            return

        # ---- A. 设置数据库 Schema (约束和索引) ----
        try:
            setup_database_schema(driver)
        except Exception as e:
            print(f"未能完成数据库Schema设置，将中止数据加载: {e}")
            return

        # ---- B. 加载主CSV文件数据 ----
        csv_filepath = 'policy_data_with_quantitative_info_v6_formatted.csv'
        try:
            df_policies = pd.read_csv(csv_filepath, dtype=str).fillna('')
        except FileNotFoundError:
            print(f"错误: 主数据文件 {csv_filepath} 未找到。")
            return

        print(f"开始从 {csv_filepath} 加载政策数据...")
        with driver.session(database="neo4j") as session:
            processed_rows = 0
            for index, row in df_policies.iterrows():
                fabao_citation = row.get('FabaoCitation', '').strip()
                if not fabao_citation:
                    print(f"警告: 第 {index + 2} 行缺少 FabaoCitation，跳过此行。")
                    continue

                policy_data = {
                    "fabaoCitation": fabao_citation,
                    "title": row.get('Title', ''),
                    "documentNumber": row.get('DocumentNumber', ''),
                    "announceDate": process_date_format(row.get('AnnounceDate', '')),
                    "implementDate": process_date_format(row.get('ImplementDate', '')),
                    "policyLevel": row.get('Level', ''),
                    "validationStatus": row.get('Validation', ''),
                    "fullText": row.get('FullText', ''),
                    "issuingBodyFullName": row.get('IssuingBodyFullName', ''),
                    "issuingBodyShortName": row.get('IssuingBodyShortName', '')
                }
                session.execute_write(create_policy_and_issuer_tx, policy_data)

                multi_value_fields_map = {
                    'PolicyTopic': ("PolicyTopic", "HAS_TOPIC", "name"),
                    'TargetBeneficiary': ("TargetBeneficiary", "TARGETS_BENEFICIARY", "name"),
                    'GeographicRegion': ("GeographicRegion", "APPLICABLE_IN", "name"),
                }
                for csv_col, (label, rel_type, name_field) in multi_value_fields_map.items():
                    if row.get(csv_col, ''):
                        for item_name in str(row.get(csv_col, '')).split(';'):
                            session.execute_write(link_policy_to_simple_node_tx, fabao_citation, label, rel_type,
                                                  name_field, item_name)

                if row.get('IndustryFocus', ''):
                    for industry_name_raw in str(row.get('IndustryFocus', '')).split(';'):
                        industry_name = industry_name_raw.strip()
                        if industry_name:
                            industry_code = INDUSTRY_CODE_MAPPING.get(industry_name)
                            session.execute_write(link_policy_to_industry_focus_tx, fabao_citation, industry_name,
                                                  industry_code)

                all_parsed_quant_details = parse_quantitative_info_updated(row.get('QuantitativeInfo', ''))
                if row.get('PolicyTool', ''):
                    for tool_name_raw in str(row.get('PolicyTool', '')).split(';'):
                        tool_name = tool_name_raw.strip()
                        if tool_name:
                            current_tool_detail = all_parsed_quant_details.get(tool_name)
                            session.execute_write(link_policy_to_tool_tx, fabao_citation, tool_name,
                                                  current_tool_detail)
                processed_rows += 1
            print(f"主政策数据加载完成。处理了 {processed_rows} 行有效数据。")

        # ---- C. 加载映射表数据并更新节点 ----
        policy_tool_xlsx_path = 'policy_tool.xlsx'
        try:
            df_tool_categories = pd.read_excel(policy_tool_xlsx_path, dtype=str).fillna('')
            print(f"开始从 {policy_tool_xlsx_path} 加载政策工具分类...")
            with driver.session(database="neo4j") as session:
                for index, row_map in df_tool_categories.iterrows():
                    tool_name = row_map.get('PolicyTool', '').strip()
                    category = row_map.get('Category', '').strip()
                    if tool_name:
                        session.execute_write(update_node_properties_tx,
                                              "PolicyTool", "name", tool_name,
                                              {"category": category if category else None})
            print("政策工具分类更新完成。")
        except FileNotFoundError:
            print(f"警告: 工具分类映射文件 {policy_tool_xlsx_path} 未找到。跳过此步骤。")
        except Exception as e:
            print(f"处理 {policy_tool_xlsx_path} 时出错: {e}")

        area_code_xlsx_path = 'area_code.xlsx'
        try:
            df_area_codes = pd.read_excel(area_code_xlsx_path,
                                          dtype={'Code': str, 'Name': str, 'Level': str, 'Pcode': str,
                                                 'Category': str}).fillna('')
            print(f"开始从 {area_code_xlsx_path} 加载行政区划信息...")
            with driver.session(database="neo4j") as session:
                for index, row_map in df_area_codes.iterrows():
                    area_name = row_map.get('Name', '').strip()
                    area_code = row_map.get('Code', '').strip()
                    area_level_str = row_map.get('Level', '').strip()
                    area_level_int = None
                    if area_level_str:
                        try:
                            area_level_int = int(area_level_str)
                        except ValueError:
                            print(
                                f"警告: area_code.xlsx 中区域 '{area_name}' 的 Level '{area_level_str}' 不是有效数字，将不设置Level。")
                    if area_name:
                        properties = {"code": area_code if area_code else None}
                        if area_level_int is not None:
                            properties["level"] = area_level_int
                        session.execute_write(update_node_properties_tx,
                                              "GeographicRegion", "name", area_name, properties)
            print("行政区划信息更新完成。")
        except FileNotFoundError:
            print(f"警告: 行政区划映射文件 {area_code_xlsx_path} 未找到。跳过此步骤。")
        except Exception as e:
            print(f"处理 {area_code_xlsx_path} 时出错: {e}")

        print("所有数据加载和更新操作已尝试执行。")


if __name__ == "__main__":
    main()