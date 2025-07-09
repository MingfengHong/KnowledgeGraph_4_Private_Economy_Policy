# tool_code
import os
import pandas as pd
import re

# 清理文本内容的函数 (修改版)
def clean_text_content(file_path, title):
    """
    读取文本文件，尝试移除头部信息，返回清理后的正文和识别到的发文字号。
    """
    lines = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except UnicodeDecodeError:
        try:
            with open(file_path, 'r', encoding='gbk') as f:
                lines = f.readlines()
        except Exception as e_gbk:
            print(f"\n错误: 无法使用 UTF-8 或 GBK 解码文件 {os.path.basename(file_path)}。错误信息: {e_gbk}")
            return None, None # 返回两个 None
    except FileNotFoundError:
        return None, None # 返回两个 None
    except Exception as e:
        print(f"\n错误: 处理文件 {os.path.basename(file_path)} 时发生异常: {e}")
        return None, None # 返回两个 None

    if not lines:
        return None, None # 如果文件为空

    identified_doc_num = None # 初始化识别到的发文字号
    start_index = 0
    lines_to_skip = 0
    max_header_lines = 10

    # 1. 跳过元数据行
    for i in range(min(max_header_lines, len(lines))):
        line_stripped = lines[i].strip()
        if line_stripped.startswith('【法宝引证码】') or line_stripped.startswith('原文链接：'):
            lines_to_skip = i + 1
        else:
            if i < 2: break
            elif lines_to_skip > 0: break
    start_index = lines_to_skip

    # 2. 跳过重复标题
    title_prefix = title[:15].strip()
    title_lines_found = 0
    for i in range(start_index, min(start_index + 3, len(lines))):
         line_stripped = lines[i].strip()
         if title_prefix and line_stripped.replace(" ","").startswith(title_prefix.replace(" ","")):
             lines_to_skip = i + 1
             title_lines_found += 1
         elif title_lines_found > 0: break
         elif title_lines_found == 0 and i >= start_index: break
    start_index = lines_to_skip

    # 3. *** 识别并提取发文字号 (同时兼容全角/半角括号) ***
    if start_index < len(lines):
        line_stripped = lines[start_index].strip()
        # 更新正则表达式以包含半角括号，并简化匹配逻辑
        # 匹配被 () （） 〔〕 【】 包围的内容，或以 "号" 结尾的非空字符串
        doc_num_regex = r'^\s*([（(〔【].*?[）)〕】]|\S+?号)\s*$'
        # 增加长度限制，避免误判过长的普通文本行
        if line_stripped and len(line_stripped) < 100 and re.match(doc_num_regex, line_stripped):
            identified_doc_num = line_stripped # 存储识别到的发文字号
            start_index += 1 # 跳过这一行
            # print(f"      -- 识别到发文号: {identified_doc_num}") # 调试时可取消注释

    # --- 拼接正文 ---
    content = None
    if start_index < len(lines):
        content_lines = [line.strip() for line in lines[start_index:]]
        # 过滤掉可能的空行？或者保留？暂时保留
        content = "\n".join(content_lines).strip()
        # 清理多余的空行
        if content: # 确保内容不是 None 或空字符串
             content = re.sub(r'\n\s*\n', '\n\n', content)

    # 返回正文和识别到的发文字号
    return content, identified_doc_num


# --- 从文件路径提取ID的函数 (保持不变) ---
def extract_id_from_path(filepath):
    """ 从文件路径中提取末尾18字符的ID（去除括号） """
    if filepath and isinstance(filepath, str):
        try:
            base_name = os.path.splitext(os.path.basename(filepath))[0]
            if len(base_name) >= 18:
                id_part = base_name[-18:]
                cleaned_id = id_part.strip('()')
                return cleaned_id
            else:
                 cleaned_id = base_name.strip('()')
                 return cleaned_id if cleaned_id != base_name else None
        except Exception as e:
            return None
    return None

# --- 配置区域 ---
BASE_DIR = r"C:\Users\hongm\OneDrive\桌面\民营经济促进政策\政策文本"
CATEGORIES = [
    "部门规范性文件",
    "党内法规制度",
    "地方性法规",
    "地方性规范文件",
    "地方政府规章",
    "法律",
    "行政法规"
]
OUTPUT_FILENAME_CSV = "combined_policy_data_adjusted_v2.csv" # 更新输出文件名
MAX_CELL_LENGTH = 15000
TRUNCATION_SUFFIX = "... [截断]"

# --- 主处理逻辑 ---
all_data_dfs = []
not_found_files = []
found_files_count = 0
truncated_files_count = 0
filled_doc_num_count = 0 # 新增：统计填充的发文字号数量

print(f"开始处理数据，根目录: {BASE_DIR}")
print("采用混合匹配逻辑 + 超长文本截断 + 增强发文号处理 (仅输出CSV):")
# ... [其他打印信息不变] ...

# 遍历每个分类目录
for category in CATEGORIES:
    print(f"\n--- 正在处理分类: {category} ---")
    # ... [路径检查和Excel读取代码不变] ...
    category_path = os.path.join(BASE_DIR, category)
    excel_filename = f"{category}目录.xlsx"
    text_dirname = f"{category}全文"
    excel_filepath = os.path.join(category_path, excel_filename)
    text_dirpath = os.path.join(category_path, text_dirname)

    if not os.path.isfile(excel_filepath): continue
    if not os.path.isdir(text_dirpath): continue

    try: df = pd.read_excel(excel_filepath)
    except Exception as e: continue

    # 创建查找字典和文件列表
    text_files_primary = {}
    all_file_paths_in_category = []
    try:
        # ... [遍历文件创建字典和列表的代码不变] ...
        files_in_dir = 0
        for filename in os.listdir(text_dirpath):
            if filename.lower().endswith(".txt"):
                files_in_dir += 1
                full_path = os.path.join(text_dirpath, filename)
                all_file_paths_in_category.append(full_path)
                base_name = os.path.splitext(filename)[0].strip()
                key_primary = base_name[:21].lower()
                text_files_primary[key_primary] = full_path
        print(f"  在 {text_dirname} 目录找到 {files_in_dir} 个 .txt 文件。")
    except Exception as e: continue

    # --- 遍历 Excel DataFrame 的每一行 ---
    extracted_texts = []
    matched_filepaths = []
    identified_doc_nums = [] # *** 新增列表 ***
    category_found_count = 0
    category_truncated_count = 0
    print(f"  开始处理 '{category}' 的 {len(df)} 条 Excel 条目...")

    for index, row in df.iterrows():
        # ... [获取 row_identifier 和 title 的代码不变] ...
        row_identifier = f"序号 {row.get('序号', index+2)}"
        title = str(row.get('标题', '')).strip()

        # 打印日志精简，只保留基本信息
        # print(f"\n    >> 正在处理: {category} - {row_identifier} - 标题: '{title[:30]}...'")

        if not title:
            extracted_texts.append(None)
            matched_filepaths.append(None)
            identified_doc_nums.append(None) # *** 补充 None ***
            continue

        txt_filepath = None

        # 1. 主要查找尝试
        lookup_key_primary = title[:21].lower()
        txt_filepath = text_files_primary.get(lookup_key_primary)

        # 2. 次要查找尝试
        if txt_filepath is None:
            title_lower = title.lower()
            for current_filepath in all_file_paths_in_category:
                current_basename = os.path.splitext(os.path.basename(current_filepath))[0].strip()
                current_basename_lower = current_basename.lower()
                if current_basename_lower.startswith(title_lower):
                    txt_filepath = current_filepath
                    break

        # --- 处理查找结果 ---
        if txt_filepath:
            # *** 接收两个返回值 ***
            cleaned_content, identified_doc_num = clean_text_content(txt_filepath, title)

            if cleaned_content is not None:
                # 处理截断 (保持不变)
                if len(cleaned_content) > MAX_CELL_LENGTH:
                    cleaned_content = cleaned_content[:MAX_CELL_LENGTH] + TRUNCATION_SUFFIX
                    category_truncated_count += 1
                    truncated_files_count += 1

                extracted_texts.append(cleaned_content)
                matched_filepaths.append(txt_filepath)
                identified_doc_nums.append(identified_doc_num) # *** 存储识别的文号 ***
                category_found_count += 1
            else:
                 extracted_texts.append(None)
                 matched_filepaths.append(txt_filepath) # 即使内容为空，也记录路径
                 identified_doc_nums.append(identified_doc_num) # *** 存储识别的文号 ***
        else:
            extracted_texts.append(None)
            matched_filepaths.append(None)
            identified_doc_nums.append(None) # *** 未找到文件，文号也为 None ***
            not_found_files.append({
                'Category': category,
                'Identifier': row_identifier,
                'Title': title,
                'PrimaryLookupKey': lookup_key_primary
            })

    # 添加新列到当前分类的 DataFrame
    df['全文内容'] = extracted_texts
    df['Matched_Filepath'] = matched_filepaths
    df['Identified_Doc_Num'] = identified_doc_nums # *** 添加临时文号列 ***
    all_data_dfs.append(df)
    print(f"  完成处理 {category}。获取 {category_found_count} 条文本 (其中 {category_truncated_count} 条被截断)。")

# --- 合并所有 DataFrame ---
if all_data_dfs:
    print("\n--- 开始合并和后处理 ---")
    final_df = pd.concat(all_data_dfs, ignore_index=True)
    print(f"数据合并完成。当前条目数: {len(final_df)}")

    # --- 执行微调 ---

    # *** 新增步骤：填充缺失的发文字号 ***
    print("  * 正在尝试使用提取的文号填充缺失的 '发文字号'...")
    if '发文字号' in final_df.columns and 'Identified_Doc_Num' in final_df.columns:
        # 条件：原始 '发文字号' 为空 (NaN 或 '') 且 识别出的 'Identified_Doc_Num' 不为空
        condition = (pd.isna(final_df['发文字号']) | (final_df['发文字号'].astype(str).str.strip() == '')) & \
                    (pd.notna(final_df['Identified_Doc_Num']) & (final_df['Identified_Doc_Num'].astype(str).str.strip() != ''))

        fill_count = condition.sum()
        if fill_count > 0:
            # 只对满足条件的行进行填充
            final_df.loc[condition, '发文字号'] = final_df.loc[condition, 'Identified_Doc_Num']
            filled_doc_num_count = fill_count # 更新全局计数器
            print(f"     成功使用提取的文号填充了 {fill_count} 个缺失的 '发文字号'。")
        else:
            print("     未发现可填充的缺失 '发文字号'。")

        # 删除临时的识别文号列
        final_df.drop(columns=['Identified_Doc_Num'], inplace=True)
        print("     已删除临时 'Identified_Doc_Num' 列。")
    else:
        print("     警告: 未找到 '发文字号' 或临时 'Identified_Doc_Num' 列，跳过填充步骤。")
        # 如果临时列存在但'发文字号'列不存在，仍然尝试删除临时列
        if 'Identified_Doc_Num' in final_df.columns:
            final_df.drop(columns=['Identified_Doc_Num'], inplace=True)


    # 1. 合并日期列 ('实施日期' -> '施行日期')
    print("  1. 正在合并日期列...")
    if '实施日期' in final_df.columns and '施行日期' in final_df.columns:
        final_df['施行日期'] = final_df['施行日期'].combine_first(final_df['实施日期'])
        try: # 增加错误处理，防止列不存在时出错
             final_df.drop(columns=['实施日期'], inplace=True)
        except KeyError:
             print("     尝试删除 '实施日期' 列时出错（可能已被删除或不存在）。")
        print("     完成日期合并。")
    # ... [其他日期列检查逻辑保持不变] ...
    elif '实施日期' in final_df.columns: print("     警告: 仅存在 '实施日期' 列。")
    elif '施行日期' in final_df.columns: print("     信息: 仅存在 '施行日期' 列。")
    else: print("     警告: 未找到日期列。")

    # 2. 基于 '标题' 列去重
    print("  2. 正在基于 '标题' 列去重...")
    original_count = len(final_df)
    final_df.drop_duplicates(subset=['标题'], keep='first', inplace=True)
    new_count = len(final_df)
    print(f"     去重完成。移除 {original_count - new_count} 条重复项。")


    # 3. 生成新的 '编号' 列并删除旧 '序号'
    print("  3. 正在生成新的 '编号' 列...")
    if 'Matched_Filepath' in final_df.columns:
        final_df['编号'] = final_df['Matched_Filepath'].apply(extract_id_from_path)
        try: # 增加错误处理
             final_df.drop(columns=['Matched_Filepath'], inplace=True)
        except KeyError: pass # 如果列不存在，忽略错误
        print("     已生成 '编号' 列。")

        if '序号' in final_df.columns:
            try:
                 final_df.drop(columns=['序号'], inplace=True)
                 print("     已删除旧的 '序号' 列。")
            except KeyError: pass
        else: print("     未找到旧的 '序号' 列。")
    else:
        print("     警告: 未找到 'Matched_Filepath' 列，无法生成 '编号'。")


    # 4. 调整列顺序
    print("  4. 正在调整列顺序...")
    if '编号' in final_df.columns:
        current_columns = final_df.columns.tolist()
        if '编号' in current_columns:
            current_columns.insert(0, current_columns.pop(current_columns.index('编号')))
            # 只保留实际存在的列，防止因列被删除而出错
            final_columns_order = [col for col in current_columns if col in final_df.columns]
            final_df = final_df[final_columns_order]
            print("     已调整列顺序。")
        else: print("     '编号' 列未成功创建。")
    else: print("     未找到 '编号' 列。")


    # --- 输出到 CSV 文件 ---
    print(f"\n后处理完成。最终条目数: {len(final_df)}")
    print(f"成功提取到文本内容的总条目数（去重后）: {final_df['全文内容'].notna().sum()}")
    if filled_doc_num_count > 0:
         print(f"其中，有 {filled_doc_num_count} 条记录的 '发文字号' 是从文本内容中提取并填充的。")
    # 截断计数在去重后可能不准确

    try:
        output_path_csv = os.path.join(BASE_DIR, OUTPUT_FILENAME_CSV)
        print(f"\n准备将最终结果写入 CSV 文件: {output_path_csv}")
        final_df.to_csv(output_path_csv, index=False, encoding='utf-8-sig')
        print(f"已成功将合并后的数据保存到 CSV 文件。")
    except Exception as e_csv:
        print(f"\n错误: 保存数据到 CSV 文件时出错: {e_csv}")

    # --- 打印未找到文件的摘要 ---
    if not_found_files:
        print("\n--- 摘要：处理过程中未能找到匹配文件的原始记录 ---")
        final_missing_titles = final_df[final_df['全文内容'].isna()]['标题'].tolist()
        original_not_found_count = len(not_found_files)
        final_not_found_count = len(final_missing_titles)
        print(f"  (原始记录 {original_not_found_count} 条未找到，最终输出 {final_not_found_count} 行内容为空)")

    none_content_count = final_df['全文内容'].isna().sum()
    if none_content_count > 0:
        print(f"\n注意：最终输出文件中有 {none_content_count} 行的 '全文内容' 为空。")

    print("\n数据处理及微调完成。")

else:
    print("\n未处理任何数据。")