import csv
import json
from datetime import datetime
import hashlib
import chardet

def detect_encoding(file_path):
    """检测文件编码"""
    with open(file_path, 'rb') as f:
        raw_data = f.read()
        result = chardet.detect(raw_data)
        return result['encoding']

def generate_id(prefix, index):
    """生成唯一ID"""
    return f"{prefix}_{datetime.now().strftime('%Y%m%d')}_{str(index).zfill(3)}"

def parse_phone(phone_str):
    """解析手机号，去除星号"""
    if not phone_str or phone_str.strip() == '':
        return 0
    # 假设格式为 176****5751，提取前3位和后4位
    if '****' in phone_str:
        parts = phone_str.split('****')
        return int(parts[0] + '0000' + parts[1])
    return int(phone_str.replace('-', '').replace(' ', ''))

def parse_datetime(date_str):
    """解析日期时间字符串"""
    if not date_str or date_str.strip() == '':
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        # 尝试解析 "2025/9/25 10:49" 格式
        dt = datetime.strptime(date_str, "%Y/%m/%d %H:%M")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except:
        try:
            # 尝试解析 "2025-9-25 10:49" 格式
            dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def safe_float(value, default=0.0):
    """安全转换为浮点数"""
    if value is None or value == '' or str(value).strip() == '':
        return default
    try:
        return float(str(value).strip())
    except:
        return default

def safe_str(value, default=''):
    """安全转换为字符串"""
    if value is None or value == '':
        return default
    return str(value).strip()

def csv_to_json(csv_file, output_file=None, config=None):
    """
    将CSV文件转换为设备推送JSON格式
    
    参数:
        csv_file: CSV文件路径
        output_file: 输出JSON文件路径（可选，默认为输入文件名.json）
        config: 配置字典（可选）
    """
    
    # 默认配置
    default_config = {
        'sys_flag': 'gas',              # 专项类型
        'object_type': 'OBJ_GX',       # 对象类型
        'region_code': '110000',        # 区域编码
        'project_id': 'ssxm_hrrqeq',   # 项目ID
        'equip_type': 'jcsb174',        # 设备类型
        'product_code': '0167',  # 产品编号
        'object_name': '燃气管线',  # 对象名称
        'subject_type': 'gas_pipeline'   # 主体类型
    }
    
    if config:
        default_config.update(config)
    
    # 自动检测文件编码
    try:
        file_encoding = detect_encoding(csv_file)
        print(f"检测到文件编码: {file_encoding}")
    except:
        file_encoding = 'gbk'  # 如果检测失败，尝试GBK
        print(f"编码检测失败，尝试使用: {file_encoding}")
    
    # 读取CSV文件，尝试多种编码
    equipment_list = []
    encodings_to_try = [file_encoding, 'utf-8', 'gbk', 'gb2312', 'utf-8-sig', 'latin1']
    
    for encoding in encodings_to_try:
        try:
            with open(csv_file, 'r', encoding=encoding) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # 数据清洗：只保留有设备编号的行
                    if row.get('设备编号') and str(row.get('设备编号')).strip():
                        equipment_list.append(row)
            print(f"成功使用编码 {encoding} 读取文件")
            break
        except UnicodeDecodeError:
            equipment_list = []
            continue
        except Exception as e:
            print(f"使用编码 {encoding} 时出错: {e}")
            equipment_list = []
            continue
    
    if not equipment_list:
        print("CSV文件为空或格式错误，没有找到有效的设备数据")
        return
    
    print(f"找到 {len(equipment_list)} 条有效设备数据")
    
    # 生成监测对象（通常一个项目一个对象）
    first_equip = equipment_list[0]
    object_code = f"{default_config['region_code']}_{default_config['sys_flag']}_OBJ_001"
    
    # 计算对象的平均经纬度（排除空值）
    valid_coords = [(safe_float(row.get('经度')), safe_float(row.get('纬度'))) 
                    for row in equipment_list 
                    if safe_float(row.get('经度')) != 0 and safe_float(row.get('纬度')) != 0]
    
    if valid_coords:
        avg_lon = sum(lon for lon, lat in valid_coords) / len(valid_coords)
        avg_lat = sum(lat for lon, lat in valid_coords) / len(valid_coords)
    else:
        avg_lon, avg_lat = 116.0, 40.0  # 默认坐标
    
    object_data = {
        "id": generate_id("OBJ", 1),
        "coamObjectCode": object_code,
        "coamObjectName": default_config['object_name'],
        "coamObjectType": default_config['object_type'],
        "coamObjectMechanism": safe_str(first_equip.get('施工单位'), default_config['manage_company']),
        "coamObjectRegion": default_config['region_code'],
        "coamObjectProject": default_config['project_id'],
        "coamObjectPosition": safe_str(first_equip.get('施工区域'), ''),
        "coamObjectStatus": "ENABLE",
        "coamUsable": 0,
        "coamSysFlag": default_config['sys_flag'],
        "coamSubjectType": default_config['subject_type'],
        "coamSubjectCode": object_code,
        "coamLongitude": f"{avg_lon:.6f}",
        "coamLatitude": f"{avg_lat:.6f}",
        "createBy": parse_phone(first_equip.get('手机号', '')),
        "createTime": parse_datetime(first_equip.get('拍摄时间', '')),
        "updateBy": parse_phone(first_equip.get('手机号', '')),
        "updateTime": parse_datetime(first_equip.get('拍摄时间', ''))
    }
    
    # 生成点位、设备和关系数据
    point_list = []
    equipment_vo_list = []
    relation_list = []
    
    for idx, row in enumerate(equipment_list, 1):
        equip_code = safe_str(row.get('设备编号'), f'EQUIP_{idx}')
        point_code = f"{object_code}_P{str(idx).zfill(3)}"
        create_time = parse_datetime(row.get('拍摄时间', ''))
        phone = parse_phone(row.get('手机号', ''))
        longitude = safe_str(row.get('经度'), '0')
        latitude = safe_str(row.get('纬度'), '0')
        altitude = safe_str(row.get('海拔'), '0')
        diameter = safe_str(row.get('管径直径'), '')
        coupling = safe_str(row.get('管箍'), '无')
        location = safe_str(row.get('安装位置'), f'位置{idx}')
        area = safe_str(row.get('施工区域'), '')
        company = safe_str(row.get('施工单位'), default_config['manage_company'])
        
        # 点位数据
        point_data = {
            "id": generate_id("POINT", idx),
            "coamPointCode": point_code,
            "coamPointName": f"{location}监测点",
            "coamPointStatus": "ENABLE",
            "coamPointAlarm": "是",
            "coamPointThreshold": "是",
            "coamPointObjectCode": object_code,
            "coamPointRegion": default_config['region_code'],
            "coamPointPosition": location,
            "coamUsable": 0,
            "coamLongitude": longitude,
            "coamLatitude": latitude,
            "coamPointType": default_config['subject_type'],
            "extraField": json.dumps({
                "altitude": altitude,
                "pipelineDiameter": diameter,
                "coupling": coupling
            }, ensure_ascii=False),
            "createBy": phone,
            "createTime": create_time,
            "updateBy": phone,
            "updateTime": create_time
        }
        point_list.append(point_data)
        
        # 设备数据
        equipment_data = {
            "id": generate_id("EQUIP", idx),
            "coamEquipName": f"{location}监测设备",
            "coamEquipCode": equip_code,
            "coamDynamicEquipCode": equip_code,
            "coamParentId": "",
            "coamEquipType": default_config['equip_type'],
            "coamManageCompany": company,
            "coamManufacturer": "",
            "coamModelnum": "",
            "coamSysFlag": default_config['sys_flag'],
            "coamLocation": location,
            "coamEquipStatus": "equipStatus0",
            "coamDataSource": "0",
            "coamIsMaintain": "0",
            "coamExamineStatus": "3",
            "coamProjectId": default_config['project_id'],
            "coamMonitorObjectCode": object_code,
            "coamMonitorPointCode": point_code,
            "coamPointBindDate": create_time,
            "coamCmUsable": 0,
            "deviceDetail": json.dumps({
                "pipelineDiameter": diameter,
                "coupling": coupling,
                "altitude": altitude
            }, ensure_ascii=False),
            "productCode": default_config['product_code'],
            "imei": equip_code,
            "isSelfBuilt": "0",
            "createBy": phone,
            "createTime": create_time,
            "updateBy": phone,
            "updateTime": create_time
        }
        equipment_vo_list.append(equipment_data)
        
        # 设备点位关系数据
        relation_data = {
            "id": generate_id("REL", idx),
            "coamEquipCode": equip_code,
            "coamPointCode": point_code,
            "coamStatus": "1",
            "coamAddtime": create_time,
            "coamUsable": 0,
            "coamDescript": "设备与点位绑定",
            "createBy": phone,
            "createTime": create_time,
            "updateBy": phone,
            "updateTime": create_time
        }
        relation_list.append(relation_data)
    
    # 组装最终JSON
    result = {
        "objectPushVoList": [object_data],
        "pointPushVoList": point_list,
        "equipmentPushVoList": equipment_vo_list,
        "equipRlPushVoList": relation_list
    }
    
    # 输出JSON
    if output_file is None:
        output_file = csv_file.rsplit('.', 1)[0] + '.json'
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)
    
    print(f"\n{'='*50}")
    print(f"转换完成！")
    print(f"{'='*50}")
    print(f"输入文件: {csv_file}")
    print(f"输出文件: {output_file}")
    print(f"处理设备数量: {len(equipment_list)}")
    print(f"生成监测对象数量: 1")
    print(f"生成监测点位数量: {len(point_list)}")
    print(f"生成设备数量: {len(equipment_vo_list)}")
    print(f"生成关系数量: {len(relation_list)}")
    print(f"{'='*50}\n")
    
    return result


if __name__ == "__main__":
    # 使用示例
    
    # 首先安装chardet库: pip install chardet
    
    # 配置参数（根据实际情况修改）
    config = {
        'sys_flag': 'gas',              # 专项类型
        'object_type': 'OBJ_GX',       # 对象类型
        'region_code': '110000',        # 区域编码
        'project_id': 'ssxm_hrrqeq',   # 项目ID
        'equip_type': 'jcsb174',        # 设备类型
        'product_code': '0167',  # 产品编号
        'object_name': '燃气管线',  # 对象名称
        'subject_type': 'gas_pipeline'   # 主体类型
    }
    
    # 转换CSV到JSON
    csv_to_json('D:\辰安科技\项目\怀柔燃气\部署\振动监测\equipment_data.csv', 'D:\辰安科技\项目\怀柔燃气\部署\振动监测\output.json', config)
    
    # 或者使用默认配置
    # csv_to_json('equipment_data.csv')
