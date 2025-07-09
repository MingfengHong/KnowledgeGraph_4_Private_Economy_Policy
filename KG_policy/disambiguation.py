import csv
import os
from openai import OpenAI
import time

# --- Configuration ---
INPUT_CSV_FILE = 'policy_data.csv'
OUTPUT_CSV_FILE = 'policy_data_standardized_v4.csv' # Output filename remains as per your script
TARGET_COLUMN_NAME = '制定机关'
NEW_COLUMN_NAME = '制定机关_标准化'

# --- IMPORTANT SECURITY NOTE ---
# The API key below is from your provided script.
# Ensure this is a placeholder or that you are managing its security appropriately.
DEEPSEEK_API_KEY = "sk-xxx" # <--- User-provided key, ensure it's intended or replace
DEEPSEEK_BASE_URL = "https://api.deepseek.com"
LLM_MODEL = "deepseek-chat"

API_CALL_DELAY_SECONDS = 0.5

# --- Central Government & CCP Bodies List (for LLM prompt reference & direct match) ---
CENTRAL_BODIES = set([
    # State Council System
    "中华人民共和国国务院办公厅", "中华人民共和国外交部", "中华人民共和国国防部",
    "中华人民共和国国家发展和改革委员会", "中华人民共和国教育部", "中华人民共和国科学技术部",
    "中华人民共和国工业和信息化部", "中华人民共和国国家民族事务委员会", "中华人民共和国公安部",
    "中华人民共和国国家安全部", "中华人民共和国民政部", "中华人民共和国司法部",
    "中华人民共和国财政部", "中华人民共和国人力资源和社会保障部", "中华人民共和国自然资源部",
    "中华人民共和国生态环境部", "中华人民共和国住房和城乡建设部", "中华人民共和国交通运输部",
    "中华人民共和国水利部", "中华人民共和国农业农村部", "中华人民共和国商务部",
    "中华人民共和国文化和旅游部", "中华人民共和国国家卫生健康委员会", "中华人民共和国退役军人事务部",
    "中华人民共和国应急管理部", "中国人民银行", "中华人民共和国审计署",
    "国务院国有资产监督管理委员会", "中华人民共和国海关总署", "国家税务总局",
    "国家市场监督管理总局", "国家金融监督管理总局", "中国证券监督管理委员会",
    "国家广播电视总局", "国家体育总局", "国家信访局", "国家统计局",
    "国家知识产权局", "国家国际发展合作署", "国家医疗保障局", "国务院参事室",
    "国家机关事务管理局", "国务院研究室",
    "国务院台湾事务办公室", "国家互联网信息办公室",
    "新华通讯社", "中国科学院", "中国社会科学院", "中国工程院",
    "国务院发展研究中心", "中央广播电视总台", "中国气象局", "国家行政学院",
    "全国人民代表大会", "全国人民代表大会常务委员会",
    # CCP Central Departments
    "中共中央纪律检查委员会", "中华人民共和国国家监察委员会", "中共中央办公厅",
    "中共中央组织部", "中共中央宣传部", "中共中央统一战线工作部",
    "中共中央对外联络部", "中共中央政法委员会", "中共中央政策研究室",
    "中央全面深化改革委员会办公室", "中央国家安全委员会办公室", "中央网络安全和信息化委员会办公室",
    "中央军民融合发展委员会办公室", "中共中央台湾工作办公室", "中央财经委员会办公室",
    "中央外事工作委员会办公室", "中央机构编制委员会办公室", "中国共产党中央委员会中央和国家机关工作委员会"
])

# --- Zhejiang Local Institutions List (for LLM prompt reference & direct match) ---
ZHEJIANG_BODIES = set([
    # Zhejiang Government
    "浙江省人民政府办公厅", "浙江省发展和改革委员会", "浙江省经济和信息化厅", "浙江省教育厅",
    "浙江省科学技术厅", "浙江省民族宗教事务委员会", "浙江省公安厅", "浙江省民政厅",
    "浙江省司法厅", "浙江省财政厅", "浙江省人力资源和社会保障厅", "浙江省自然资源厅",
    "浙江省生态环境厅", "浙江省住房和城乡建设厅", "浙江省交通运输厅", "浙江省水利厅",
    "浙江省农业农村厅", "浙江省海洋经济发展厅", "浙江省商务厅", "浙江省文化广电和旅游厅",
    "浙江省卫生健康委员会", "浙江省退役军人事务厅", "浙江省应急管理厅", "浙江省审计厅",
    "浙江省人民政府外事办公室", "浙江省人民政府国有资产监督管理委员会", "浙江省市场监督管理局",
    "浙江省体育局", "浙江省统计局", "浙江省机关事务管理局", "浙江省医疗保障局",
    "浙江省国防动员办公室", "浙江省数据局", "浙江省粮食和物资储备局", "浙江省能源局",
    "浙江省监狱管理局", "浙江省林业局", "浙江省文物局", "浙江省药品监督管理局",
    "浙江省疾病预防控制局", "浙江省供销合作社联合社",
    # Zhejiang CCP Committee
    "中共浙江省委组织部", "中共浙江省委宣传部", "中共浙江省委统一战线工作部",
    "中共浙江省委政法委员会", "中共浙江省委政策研究室", "中共浙江省委全面深化改革委员会办公室",
    "中共浙江省委网络安全和信息化委员会办公室", "中共浙江省委机构编制委员会办公室",
    "中共浙江省委军民融合发展委员会办公室", "中共浙江省委台湾工作办公室", "中共浙江省委信访局",
    "中共浙江省委巡视工作领导小组办公室", "中共浙江省委老干部局", "中共浙江省委直属机关工作委员会"
])

# Combine central and specified local bodies for the comprehensive direct-match list
ALL_KNOWN_CANONICAL_NAMES = CENTRAL_BODIES.union(ZHEJIANG_BODIES)

# --- LLM Prompt Template ---
# The {central_bodies_list_str} and {zhejiang_bodies_list_str} in the prompt
# will be populated from CENTRAL_BODIES and ZHEJIANG_BODIES respectively.
LLM_PROMPT_TEMPLATE = """
你是一位专门研究中国政府和中国共产党（CCP）机构结构的专家助手。你的任务是根据所提供政府机构或党组织名称的当前状态、特定性质以及与其他机构的官方关系，将其标准化。

**通用原则：统一使用官方全称**
在你的回答中，所有机构名称——无论是正在标准化的主要机构，还是任何相关机构（例如规则A中的被挂牌单位、规则B中的原机构名或新机构名）——都必须以其**完整且官方的当前（或规则中指明的历史上的）全称**呈现。除非缩写本身是官方全称的一部分，否则应避免使用缩写。

请严格遵守以下规则。规则的顺序代表其适用优先级。

**规则A：挂牌机构**
如果输入的名称指向一个当前与某个“被挂牌单位”关联的“挂牌单位”，并且该挂牌单位通常以其自身名义对外发文：
输出格式：“<挂牌单位>;<被挂牌单位>”
  - “<挂牌单位>”是挂牌单位的**当前官方全称**。
  - “<被挂牌单位>”是被挂牌单位的**当前官方全称**。（如果该被挂牌单位是人民代表大会相关机构，其名称必须按照规则C格式化）。
示例（如果挂牌单位匹配，优先使用这些特定的被挂牌单位，并确保所有名称均为官方全称）：
    - 国家语言文字工作委员会;中华人民共和国教育部
    - 国家航天局;中华人民共和国工业和信息化部
    - 国家原子能机构;中华人民共和国工业和信息化部
    - 国家外国专家局;中华人民共和国人力资源和社会保障部
    - 国家海洋局;中华人民共和国自然资源部
    - 国家核安全局;中华人民共和国生态环境部
    - 国家反垄断局;国家市场监督管理总局
    - 国家认证认可监督管理委员会;国家市场监督管理总局
    - 国家标准化管理委员会;国家市场监督管理总局
    - 国家公务员局;中共中央组织部
    - 国务院新闻办公室;中共中央宣传部
    - 国家新闻出版署;中共中央宣传部 （如果输入为国家版权局，也应按此例输出：国家版权局;中共中央宣传部）
    - 国家电影局;中共中央宣传部
    - 国家宗教事务局;中共中央统一战线工作部
    - 国务院侨务办公室;中共中央统一战线工作部
    - 国家乡村振兴局;中华人民共和国农业农村部 （请注意：核实国家乡村振兴局的当前状态。如果该局已被完全合并/更名，则规则B1可能适用于该原挂牌单位。）
    - 国务院港澳事务办公室;中共中央港澳工作办公室 （如果输入的是“国务院港澳事务办公室”，并且它被视为在“中共中央港澳工作办公室”挂牌）

如果输入的名称是一个本身已成历史（已更名、合并、撤销）的挂牌单位，则规则B1或B2适用于该挂牌单位本身。“;”格式仅用于当前活跃的挂牌关系，即挂牌实体当前存在并以其自身名义运作。

**规则B：机构状态（已更名、合并、已撤销、当前有效）**
如果规则A不适用，或者规则A的处理结果指向一个历史上的挂牌单位，则应用这些规则。

B1. 如果机构（或历史上的挂牌单位）已**更名或合并**入新的机构：
    输出格式：“<新机构名> (含原<旧机构名>)”
    - “<新机构名>”是新机构的**当前官方全称**（如果是人民代表大会相关机构，则按照规则C格式化）。
    - “<旧机构名>”是原机构的**此前官方全称**（如果是人民代表大会相关机构，则按照规则C格式化）。

B2. 如果机构（或历史上的挂牌单位）已被**撤销**：
    输出格式：“<原机构名> (已撤销)”
    - “<原机构名>”是被撤销机构的**官方全称**（如果是人民代表大会相关机构，则按照规则C格式化）。

B3. 如果机构**当前有效**，并且不是规则A所涵盖的挂牌单位，并且输入名称是缩写、通用名或轻微过时的变体（但不是规则B1所涵盖的正式更名/合并前的旧称）：
    输出格式：其**当前官方全称**。（例如，如果输入为“中共中央纪律检查委员会、中华人民共和国国家监察委员会机关”，并且这是一个当前的官方合署名称，则输出此名称）。

**规则C：人民代表大会统一名称规则**
对于任何被识别为人民代表大会（人大）或其常务委员会（人大常委会）的机构（无论中央、省、市、县等任何级别），其标准化输出名称**必须**使用统一格式：“<行政层级和名称>人民代表大会 (含常务委员会)”。
  - “<行政层级和名称>”部分必须是**完整官方的行政区划前缀**（例如：“北京市”、“全国”）。
  - 例如，输入指北京市人民代表大会或其常务委员会，应标准化为“北京市人民代表大会 (含常务委员会)”。对于国家级，则为“全国人民代表大会 (含常务委员会)”。
  - 此规则统一了人民代表大会相关机构的表述方式，并适用于最终输出中出现的任何人民代表大会名称，无论是作为<挂牌单位>、<被挂牌单位>、<新机构名>、<旧机构名>、<原机构名>，还是规则B3的直接结果。

**参考列表：主要现行中央政府与中共中央机构**
此列表有助于识别*中央*层面实体的官方名称。此列表包含官方基础名称。
--- START CENTRAL BODIES LIST ---
{central_bodies_list_str}
--- END CENTRAL BODIES LIST ---

**参考列表：浙江省主要政府与中共党委机构示例**
此列表提供了浙江省层面一些机构的官方名称示例，以辅助地方机构的标准化。
--- START ZHEJIANG BODIES LIST ---
{zhejiang_bodies_list_str}
--- END ZHEJIANG BODIES LIST ---

**应用指南**
- **优先级：** 首先评估规则A。如果不适用，则继续规则B。规则C是一个名称格式化规则，当人民代表大会相关机构的名称作为规则A或B生成的输出的一部分时适用。
- **“一个机构两块牌子”：** 如果输入的名称是某个当前活动实体的两个并列官方名称之一（例如：`中央网络安全和信息化委员会办公室（国家互联网信息办公室）`、`中共中央台湾工作办公室（国务院台湾事务办公室）`、`中共中央政策研究室（中央全面深化改革委员会办公室）`、`中央党校（国家行政学院）`【注意此例中名称的顺序，应以官方为准，如“中共中央党校（国家行政学院）”】），并且这不属于规则A描述的“加挂牌子”的情况，那么如果输入的是当前官方全称，则直接输出该官方全称。如果输入的是缩写，则提供被缩写的官方全称。这种情况通常不应使用规则A的“;”格式。
- **地方机构与地方党委机构：** 对于地方政府机构（例如，省级、市级、县级政府的部门）和地方党委机构（例如，省级、市级、县级党委的部门），如果规则A（挂牌）或规则B（状态变更）不适用，目标是输出其当前的官方全称。请参考上方提供的浙江省机构列表作为地方机构官方名称的格式参考。确保正确的行政层级，并且如果是人民代表大会相关机构，则应用规则C。

输入的政府机构或党组织名称：“{authority_name}”

请*仅*以指定格式返回标准化名称，不要添加任何解释或介绍性文字。

标准化名称：
"""

# Generate the string for the prompt using CENTRAL_BODIES
central_bodies_for_prompt_list_string = "\n".join(sorted(list(CENTRAL_BODIES)))

# Generate the string for the prompt using ZHEJIANG_BODIES
zhejiang_bodies_for_prompt_list_string = "\n".join(sorted(list(ZHEJIANG_BODIES)))


try:
    client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_BASE_URL)
except Exception as e:
    print(f"Error initializing OpenAI client: {e}")
    exit()

def get_standardized_name(authority_name_raw, llm_client):
    if not authority_name_raw or not authority_name_raw.strip():
        print("Warning: Empty authority name found. Skipping standardization.")
        return ""
    authority_name = authority_name_raw.strip()

    # Use the combined list for direct match optimization
    if authority_name in ALL_KNOWN_CANONICAL_NAMES:
        return authority_name

    prompt = LLM_PROMPT_TEMPLATE.format(
        central_bodies_list_str=central_bodies_for_prompt_list_string,
        zhejiang_bodies_list_str=zhejiang_bodies_for_prompt_list_string, # Added Zhejiang bodies list
        authority_name=authority_name
    )
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = llm_client.chat.completions.create(
                model=LLM_MODEL,
                messages=[{"role": "user", "content": prompt}],
                stream=False,
                temperature=0.0,
                max_tokens=150
            )
            standardized_name = response.choices[0].message.content.strip()
            if not standardized_name or len(standardized_name) > 150:
                 print(f"Warning: Unusual response for '{authority_name}': '{standardized_name}'. Retrying...")
                 raise ValueError("Unusual or empty response received")
            if API_CALL_DELAY_SECONDS > 0:
                time.sleep(API_CALL_DELAY_SECONDS)
            return standardized_name
        except Exception as e:
            print(f"Error calling LLM for '{authority_name}' (Attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt) # Exponential backoff
            else:
                print(f"Failed to standardize '{authority_name}' after {max_retries} attempts.")
                return f"ERROR: Could not standardize ({authority_name})"
    return f"ERROR: Could not standardize ({authority_name})" # Fallback after all retries

# --- Main Processing Logic ---
try:
    with open(INPUT_CSV_FILE, 'r', encoding='utf-8', newline='') as infile, \
         open(OUTPUT_CSV_FILE, 'w', encoding='utf-8', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        header = next(reader)
        try:
            target_col_index = header.index(TARGET_COLUMN_NAME)
        except ValueError:
            print(f"Error: Column '{TARGET_COLUMN_NAME}' not found in '{INPUT_CSV_FILE}'. Available columns: {header}")
            exit()
        new_header = header + [NEW_COLUMN_NAME]
        writer.writerow(new_header)
        print(f"Starting standardization. Input: '{INPUT_CSV_FILE}', Output: '{OUTPUT_CSV_FILE}'")
        processed_count = 0
        for i, row in enumerate(reader):
            if not row: continue # Skip empty rows
            try:
                original_authority = row[target_col_index]
                print(f"Processing row {i+2}: Original='{original_authority}'") # i is 0-based for data rows
                standardized_authority = get_standardized_name(original_authority, client)
                print(f"  -> Standardized='{standardized_authority}'")
                output_row = row + [standardized_authority]
                writer.writerow(output_row)
                processed_count += 1
            except IndexError:
                print(f"Warning: Row {i+2} has fewer columns than expected. Skipping.")
                writer.writerow(row + ["ERROR: Malformed row"])
            except Exception as e:
                 print(f"Error processing row {i+2} ('{original_authority}'): {e}")
                 writer.writerow(row + [f"ERROR: Processing failed ({original_authority})"])
        print(f"\nProcessing finished. {processed_count} data rows processed.")
        print(f"Standardized data saved to '{OUTPUT_CSV_FILE}'")
except FileNotFoundError:
    print(f"Error: Input file '{INPUT_CSV_FILE}' not found.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")