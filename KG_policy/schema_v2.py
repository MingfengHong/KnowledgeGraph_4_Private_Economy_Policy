import pandas as pd
from neo4j import GraphDatabase, basic_auth
import re

# --- 1. Neo4j 连接配置 ---
URI = "neo4j://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "88888888"  # 请确保使用您的密码

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

def clean_quantitative_info(text_data):
    """
    清理QuantitativeInfo列，移除知识抽取失败的特定提示文本。
    """
    if not text_data or pd.isna(text_data):
        return ""

    invalid_substrings = [
        "政策工具缺失或为空",
        "未找到可处理的政策工具（均未在格式映射中定义或原始列表为空）"
    ]
    cleaned_text = str(text_data).strip()
    if cleaned_text in invalid_substrings or cleaned_text == '""':
        return ""
    return cleaned_text


def parse_quantitative_info(quantitative_info_str):
    """
    解析 QuantitativeInfo 字符串。
    格式: "工具A(信息A); 工具B(信息B)"
    内部信息: "组件1, 组件2"
    返回: 字典 {"工具A": "信息A", "工具B": "信息B"}
    """
    if not quantitative_info_str or pd.isna(quantitative_info_str):
        return {}

    parsed_details = {}
    tools_info_parts = re.split(r';\s*', quantitative_info_str.strip())

    for tool_info_part in tools_info_parts:
        if not tool_info_part.strip():
            continue
        match = re.match(r'^(.*?)\(([^)]*)\)$', tool_info_part.strip())
        if match:
            tool_name = match.group(1).strip()
            detail_string = match.group(2).strip()
            if tool_name:
                parsed_details[tool_name] = detail_string
    return parsed_details


def process_date_format(date_str):
    """
    将日期字符串或Excel数字日期转换为 'YYYY-MM-DD' 格式。
    """
    if date_str and isinstance(date_str, str) and re.match(r'^\d{4}\.\d{2}\.\d{2}$', date_str.strip()):
        return date_str.strip().replace('.', '-')
    elif date_str and isinstance(date_str, (int, float)) and not pd.isna(date_str):
        try:
            return pd.to_datetime(date_str, unit='D', origin='1899-12-30').strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            pass
    return None


# --- 4. Neo4j 事务函数 ---

def create_policy_tx(tx, policy_data):
    """
    仅创建或更新政策（Policy）节点本身。
    """
    query = (
        "MERGE (p:Policy {fabaoCitation: $fabaoCitation}) "
        "ON CREATE SET "
        "    p.title = $title, p.documentNumber = $documentNumber, "
        "    p.announceDate = CASE WHEN $announceDate IS NOT NULL THEN date($announceDate) ELSE null END, "
        "    p.implementDate = CASE WHEN $implementDate IS NOT NULL THEN date($implementDate) ELSE null END, "
        "    p.policyLevel = $policyLevel, p.validationStatus = $validationStatus, p.fullText = $fullText "
        "ON MATCH SET "
        "    p.title = $title, p.documentNumber = $documentNumber, "
        "    p.announceDate = CASE WHEN $announceDate IS NOT NULL THEN date($announceDate) ELSE null END, "
        "    p.implementDate = CASE WHEN $implementDate IS NOT NULL THEN date($implementDate) ELSE null END, "
        "    p.policyLevel = $policyLevel, p.validationStatus = $validationStatus, p.fullText = $fullText "
    )
    tx.run(query, **policy_data)


def link_policy_to_issuer_tx(tx, fabao_citation, full_name, short_name):
    """
    创建颁布机构（IssuingBody）节点并关联到政策。
    """
    if not full_name:
        return

    query = (
        "MATCH (p:Policy {fabaoCitation: $fabao_citation}) "
        "MERGE (ib:IssuingBody {fullName: $full_name}) "
        "ON CREATE SET ib.shortName = $short_name "
        "ON MATCH SET ib.shortName = $short_name "
        "MERGE (p)-[:ISSUED_BY]->(ib)"
    )
    tx.run(query, fabao_citation=fabao_citation, full_name=full_name, short_name=short_name)


def link_policy_to_simple_node_tx(tx, fabao_citation, node_label, relationship_type, node_name_field, node_name_value):
    """
    通用函数，用于链接政策到一个简单的、只有一个name字段的节点。
    """
    if not node_name_value or (isinstance(node_name_value, float) and pd.isna(node_name_value)):
        return
    node_name_value_clean = str(node_name_value).strip()
    if not node_name_value_clean:
        return

    query = (
        f"MATCH (p:Policy {{fabaoCitation: $fabao_citation}}) "
        f"MERGE (n:{node_label} {{{node_name_field}: $node_name_value}}) "
        f"MERGE (p)-[:{relationship_type}]->(n)"
    )
    tx.run(query, fabao_citation=fabao_citation, node_name_value=node_name_value_clean)


def link_policy_to_industry_focus_tx(tx, fabao_citation, industry_name, industry_code):
    """
    链接政策到行业侧重节点，并设置其行业代码。
    """
    if not industry_name or (isinstance(industry_name, float) and pd.isna(industry_name)):
        return
    industry_name_clean = str(industry_name).strip()
    if not industry_name_clean:
        return

    params = {
        "fabao_citation": fabao_citation,
        "industry_name": industry_name_clean,
        "industry_code": industry_code if industry_code else None
    }

    query = (
        "MATCH (p:Policy {fabaoCitation: $fabao_citation}) "
        "MERGE (indf:IndustryFocus {name: $industry_name}) "
        "SET indf.code = $industry_code "
        "MERGE (p)-[:FOCUSES_ON_INDUSTRY]->(indf)"
    )
    tx.run(query, **params)


def link_policy_to_tool_tx(tx, fabao_citation, tool_name, quantitative_detail_value):
    """
    链接政策到政策工具节点，并设置关系的量化信息属性。
    """
    if not tool_name or (isinstance(tool_name, float) and pd.isna(tool_name)):
        return
    tool_name_clean = str(tool_name).strip()
    if not tool_name_clean:
        return

    detail_value_to_set = quantitative_detail_value if quantitative_detail_value else None

    query = (
        "MATCH (p:Policy {fabaoCitation: $fabao_citation}) "
        "MERGE (ptool:PolicyTool {name: $tool_name}) "
        "MERGE (p)-[r:APPLIES_TOOL]->(ptool) "
        "SET r.quantitativeDetail = $quantitative_detail_value"
    )
    tx.run(query, fabao_citation=fabao_citation, tool_name=tool_name_clean,
           quantitative_detail_value=detail_value_to_set)


def update_node_properties_tx(tx, node_label, node_identifier_field, node_identifier_value, properties_to_set):
    """
    通用函数，用于从映射表更新已存在节点的属性。
    """
    if not node_identifier_value or (isinstance(node_identifier_value, float) and pd.isna(node_identifier_value)):
        return
    node_identifier_value_clean = str(node_identifier_value).strip()
    if not node_identifier_value_clean:
        return

    set_clauses = []
    valid_params = {"node_identifier_value": node_identifier_value_clean}

    for key, value in properties_to_set.items():
        if value is None:
            set_clauses.append(f"n.{key} = null")
        elif not (isinstance(value, float) and pd.isna(value)) and str(value).strip() != '':
            set_clauses.append(f"n.{key} = ${key}")
            valid_params[key] = value
        elif str(value).strip() == '':
            set_clauses.append(f"n.{key} = null")

    if not set_clauses:
        return

    query = (
        f"MATCH (n:{node_label} {{{node_identifier_field}: $node_identifier_value}}) "
        f"SET {', '.join(set_clauses)}"
    )
    tx.run(query, **valid_params)


def link_region_to_parent_tx(tx, child_region_name, parent_region_code):
    """
    创建 GeographicRegion 节点之间的层级关系 (子区域 IS_SUBREGION_OF 父区域)。
    子区域通过名称匹配，父区域通过代码 (Pcode) 匹配。
    """
    if not child_region_name or not parent_region_code:
        return

    query = (
        "MATCH (child:GeographicRegion {name: $child_name}) "
        "MATCH (parent:GeographicRegion {code: $parent_code}) "
        "MERGE (child)-[:IS_SUBREGION_OF]->(parent)"
    )
    tx.run(query, child_name=child_region_name, parent_code=parent_region_code)


# --- 5. 数据库Schema设置函数 ---
def setup_database_schema(driver):
    """
    在 Neo4j 数据库中创建约束和索引，确保操作的幂等性。
    """
    print("开始设置数据库Schema (约束和索引)...")
    with driver.session(database="neo4j") as session:
        try:
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

        try:
            setup_database_schema(driver)
        except Exception as e:
            print(f"未能完成数据库Schema设置，将中止数据加载: {e}")
            return

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
                    "title": row.get('Title', '').strip(),
                    "documentNumber": row.get('DocumentNumber', '').strip(),
                    "announceDate": process_date_format(row.get('AnnounceDate', '').strip()),
                    "implementDate": process_date_format(row.get('ImplementDate', '').strip()),
                    "policyLevel": row.get('Level', '').strip(),
                    "validationStatus": row.get('Validation', '').strip(),
                    "fullText": row.get('FullText', '').strip(),
                }
                session.execute_write(create_policy_tx, policy_data)

                full_names_str = row.get('IssuingBodyFullName', '')
                short_names_str = row.get('IssuingBodyShortName', '')
                full_names = [fn.strip() for fn in str(full_names_str).split(';') if fn.strip()]
                short_names = [sn.strip() for sn in str(short_names_str).split(';') if sn.strip()]

                if len(full_names) != len(short_names) and full_names:
                    print(
                        f"警告 (行 {index + 2}, FabaoCitation: {fabao_citation}): IssuingBodyFullName ({len(full_names)}个) 和 IssuingBodyShortName ({len(short_names)}个) 数量不匹配。")

                for i, full_name_item in enumerate(full_names):
                    short_name_item = short_names[i] if i < len(short_names) else ''
                    session.execute_write(link_policy_to_issuer_tx, fabao_citation, full_name_item, short_name_item)

                multi_value_fields_map = {
                    'PolicyTopic': ("PolicyTopic", "HAS_TOPIC", "name"),
                    'TargetBeneficiary': ("TargetBeneficiary", "TARGETS_BENEFICIARY", "name"),
                    'GeographicRegion': ("GeographicRegion", "APPLICABLE_IN", "name"),
                }
                for csv_col, (label, rel_type, name_field) in multi_value_fields_map.items():
                    cell_value = row.get(csv_col, '')
                    if cell_value:
                        for item_name in str(cell_value).split(';'):
                            cleaned_item_name = item_name.strip()
                            if cleaned_item_name:
                                session.execute_write(link_policy_to_simple_node_tx, fabao_citation, label, rel_type,
                                                      name_field, cleaned_item_name)

                industry_focus_str = row.get('IndustryFocus', '')
                if industry_focus_str:
                    for industry_name_raw in str(industry_focus_str).split(';'):
                        industry_name = industry_name_raw.strip()
                        if industry_name:
                            industry_code = INDUSTRY_CODE_MAPPING.get(industry_name)
                            session.execute_write(link_policy_to_industry_focus_tx, fabao_citation, industry_name,
                                                  industry_code)

                quantitative_info_raw = row.get('QuantitativeInfo', '')
                cleaned_quant_info_str = clean_quantitative_info(quantitative_info_raw)
                all_parsed_quant_details = parse_quantitative_info(cleaned_quant_info_str)
                policy_tool_str = row.get('PolicyTool', '')
                if policy_tool_str:
                    for tool_name_raw in str(policy_tool_str).split(';'):
                        tool_name = tool_name_raw.strip()
                        if tool_name:
                            current_tool_detail = all_parsed_quant_details.get(tool_name)
                            session.execute_write(link_policy_to_tool_tx, fabao_citation, tool_name,
                                                  current_tool_detail)

                processed_rows += 1
                if processed_rows % 100 == 0:
                    print(f"已处理 {processed_rows} 行主政策数据...")
            print(f"主政策数据加载完成。总共处理了 {processed_rows} 行有效数据。")

        # ---- C. 加载映射表数据并更新节点 ----
        policy_tool_xlsx_path = 'policy_tool.xlsx'
        try:
            df_tool_categories = pd.read_excel(policy_tool_xlsx_path, dtype=str).fillna('')
            print(f"开始从 {policy_tool_xlsx_path} 加载政策工具分类...")
            updated_tools_count = 0
            with driver.session(database="neo4j") as session:
                for _, row_map in df_tool_categories.iterrows():
                    tool_name = row_map.get('PolicyTool', '').strip()
                    category = row_map.get('Category', '').strip()
                    if tool_name:
                        properties_to_set = {"category": category if category else None}
                        session.execute_write(update_node_properties_tx, "PolicyTool", "name", tool_name,
                                              properties_to_set)
                        updated_tools_count += 1
            print(f"政策工具分类更新完成。尝试更新了 {updated_tools_count} 个工具。")
        except FileNotFoundError:
            print(f"警告: 工具分类映射文件 {policy_tool_xlsx_path} 未找到。跳过此步骤。")
        except Exception as e:
            print(f"处理 {policy_tool_xlsx_path} 时出错: {e}")

        area_code_xlsx_path = 'area_code.xlsx'
        try:
            df_area_codes = pd.read_excel(area_code_xlsx_path, dtype=str).fillna('')
            print(f"开始从 {area_code_xlsx_path} 加载行政区划信息并建立层级关系...")
            updated_areas_count = 0

            with driver.session(database="neo4j") as session:
                print("第一遍：更新行政区划节点属性...")
                for _, row_map in df_area_codes.iterrows():
                    area_name = row_map.get('Name', '').strip()
                    if area_name:
                        properties = {
                            "code": str(row_map.get('Code', '')).strip() or None,
                            "level": str(row_map.get('Level', '')).strip() or None
                        }
                        properties = {k: v for k, v in properties.items() if v is not None}
                        if properties:  # 只有当有实际属性需要更新时才执行
                            session.execute_write(update_node_properties_tx, "GeographicRegion", "name", area_name,
                                                  properties)
                            updated_areas_count += 1
                print(f"行政区划节点属性更新完成。尝试更新了 {updated_areas_count} 个区域的属性。")

            print("第二遍：建立行政区划层级关系...")
            linked_regions_count = 0
            skipped_top_level_regions = 0
            with driver.session(database="neo4j") as session:
                for _, row_map in df_area_codes.iterrows():
                    child_area_name = row_map.get('Name', '').strip()
                    parent_area_code = str(row_map.get('Pcode', '')).strip()

                    if child_area_name and parent_area_code:
                        if parent_area_code == "0":  # 如果Pcode为"0"，则为顶级区域，不创建父级链接
                            skipped_top_level_regions += 1
                            continue
                        session.execute_write(link_region_to_parent_tx, child_area_name, parent_area_code)
                        linked_regions_count += 1
            print(
                f"行政区划层级关系建立尝试完成。尝试链接了 {linked_regions_count} 个区域。跳过了 {skipped_top_level_regions} 个顶级区域的父级链接。")

        except FileNotFoundError:
            print(f"警告: 行政区划映射文件 {area_code_xlsx_path} 未找到。跳过此步骤。")
        except Exception as e:
            print(f"处理 {area_code_xlsx_path} 时出错: {e}")

        print("所有数据加载和更新操作已尝试执行。")


if __name__ == "__main__":
    main()
