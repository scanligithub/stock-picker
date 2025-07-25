import pandas as pd
from utils.data_loader import load_clean_hist_data, get_clean_snapshot_data

def test_historical_data_loading():
    try:
        df = load_clean_hist_data()
        assert not df.empty, "历史数据加载结果为空"
        assert '代码' in df.columns, "历史数据缺少'代码'列"
        assert df['代码'].dtype == 'object', "股票代码应为字符串类型"
        assert any('.SH' in code or '.SZ' in code for code in df['代码'].head()), "股票代码格式不正确"
        print("✅ 历史数据加载测试通过")
        print(f"   数据量: {len(df)}行")
        print(f"   代码示例: {df['代码'].iloc[0]}")
        return True
    except Exception as e:
        print(f"❌ 历史数据加载测试失败: {str(e)}")
        return False

def test_snapshot_data_loading():
    try:
        df = get_clean_snapshot_data()
        assert not df.empty, "快照数据加载结果为空"
        assert '成交量' in df.columns, "快照数据缺少'成交量'列"
        assert df['成交量'].dtype in [int, float], "成交量应为数值类型"
        assert '代码' in df.columns, "快照数据缺少'代码'列"
        print("✅ 快照数据加载测试通过")
        print(f"   数据量: {len(df)}行")
        print(f"   成交量示例: {df['成交量'].iloc[0]}")
        return True
    except Exception as e:
        print(f"❌ 快照数据加载测试失败: {str(e)}")
        return False

def test_data_consistency():
    try:
        hist_df = load_clean_hist_data()
        snap_df = get_clean_snapshot_data()
        assert not hist_df.empty and not snap_df.empty, "需要加载数据才能测试一致性"
        
        # 检查股票代码格式一致性
        hist_code_format = all('.SH' in code or '.SZ' in code for code in hist_df['代码'].head(100))
        snap_code_format = all('.SH' in code or '.SZ' in code for code in snap_df['代码'].head(100))
        assert hist_code_format and snap_code_format, "股票代码格式不一致"
        
        # 检查成交量单位一致性
        assert hist_df['成交量'].dtype == snap_df['成交量'].dtype, "成交量数据类型不一致"
        
        print("✅ 数据一致性测试通过")
        return True
    except Exception as e:
        print(f"❌ 数据一致性测试失败: {str(e)}")
        return False

if __name__ == "__main__":
    print("===== 数据加载测试 ======")
    hist_result = test_historical_data_loading()
    snap_result = test_snapshot_data_loading()
    consistency_result = test_data_consistency() if hist_result and snap_result else False
    
    print("\n===== 测试总结 =====")
    print(f"历史数据加载: {'通过' if hist_result else '失败'}")
    print(f"快照数据加载: {'通过' if snap_result else '失败'}")
    print(f"数据一致性: {'通过' if consistency_result else '失败'}")
    
    if hist_result and snap_result and consistency_result:
        print("\n🎉 所有数据加载测试通过!")
        exit(0)
    else:
        print("\n❌ 部分测试未通过，请检查错误信息。")
        exit(1)