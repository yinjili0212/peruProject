from datetime import datetime

def strChangeTime(time_str):
    # 这个函数应该负责将字符串转换为 datetime 对象
    # 这里假设时间字符串的格式是 "%Y-%m-%d %H:%M:%S.%f"
    return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f")

def wholeHourTime(inputTime, next_hour=False):
    if isinstance(inputTime, str):
        inputTime = strChangeTime(inputTime)
    elif isinstance(inputTime, datetime):
        # 这里不需要额外的操作，因为 inputTime 已经是 datetime 类型
        pass
    else:
        raise ValueError("Input time must be a string or datetime object.")

    # 获取当前小时数
    current_hour = inputTime.hour

    # 计算整点时间或下一个整点时间
    if next_hour:
        # 如果需要下一个整点时间，且当前不是23点，则小时数加1
        if current_hour < 23:
            whole_hour = inputTime.replace(hour=current_hour + 1, minute=0, second=0, microsecond=0)
        else:
            # 如果是23点，则跳到第二天的0点
            whole_hour = inputTime.replace(day=inputTime.day + 1, hour=0, minute=0, second=0, microsecond=0)
    else:
        # 如果不需要下一个整点时间，则直接获取当前整点时间
        whole_hour = inputTime.replace(minute=0, second=0, microsecond=0)

    return whole_hour

# 示例用法
now = datetime.now()
print("当前时间:", now)
print("当前整点时间:", wholeHourTime(now))
print("下一个整点时间:", wholeHourTime(now, next_hour=True))