# AMQI 2025 项目

## 项目描述

本项目是 2025 年资产管理与量化投资课程的大作业。目标是利用中国股票市场数据，构建基于市场异象的交易策略。策略的表现将对照经典的因子模型进行评估。

## 数据来源与字典

本项目数据来源于 **Tushare Pro** 接口，原始数据存储在 `data/raw_data/` 目录下，格式为 Parquet。

主要数据文件包括：

### 1. 每日指标 (`daily_basic.parquet`)

包含每日的估值和基础指标：

- `ts_code`: 股票代码
- `trade_date`: 交易日期
- `close`: 收盘价
- `turnover_rate`: 换手率
- `pe`: 市盈率
- `pb`: 市净率
- `total_mv`: 总市值
- `circ_mv`: 流通市值
- ... (更多指标请参考 `data/raw_data/data_inspection.ipynb`)

### 2. 其他数据

- `stock_basic.parquet`: 股票基础信息
- `daily.parquet`: 日线行情
- `fina_indicator.parquet`: 财务指标
- `income.parquet`: 利润表
- `balancesheet.parquet`: 资产负债表
- `cashflow.parquet`: 现金流量表
- `dividend.parquet`: 分红送股数据

## 设置说明

1. **环境设置**:
    确保已安装 Python。建议使用虚拟环境。

    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

2. **Tushare 配置**:
    在项目根目录下创建一个 `.env` 文件，并填入您的 Tushare Token：

    ```
    TUSHARE_TOKEN=your_token_here
    ```

3. **数据获取**:
    运行以下脚本下载数据：

    ```bash
    # 下载每日基础指标
    python data/data_loader/download_daily_basic.py
    
    # (可选) 运行其他下载脚本
    # python data/data_loader/download_stock_basic.py
    # ...
    ```

## 使用方法

在分析脚本中加载处理后的数据：

```python
from data.data_loader import load_stock_data

# 默认加载 daily_basic.parquet
df = load_stock_data(filename="daily_basic.parquet")
print(df.head())
```

## 项目工作流

项目采用模块化的工作流设计，分为因子挖掘、模型组装和回测分析三个部分。

### 1. 因子库 (`factor_library/`)

用于存放和开发各类因子。

- **接口定义**: 每个因子类应继承自 `BaseFactor` 并实现 `calculate` 方法。
- **输入**: 包含股票数据的完整 `DataFrame` (必须包含 `stkcd`, `year`, `month`)。
- **输出**: `pd.Series`，索引为 `['stkcd', 'year', 'month']` 的 MultiIndex。

```python
class MyFactor(BaseFactor):
    def calculate(self, df: pd.DataFrame) -> pd.Series:
        # ... 计算逻辑 ...
        return factor_values
```

### 2. 模型组装 (`model_assembly/`)

用于将多个因子组合成最终的交易信号。

- 支持线性加权等组合方式。

### 3. 回测引擎 (`backtest_engine/`)

用于测试因子或模型的表现。

- 提供 Sharpe Ratio, IC, Max Drawdown 等指标计算。
- `Backtester` 类支持基于分位数的简单的多空回测。

### 4. 分析笔记本 (`notebooks/`)

用于交互式地测试因子、可视化结果和撰写报告。

- 推荐使用 `01_factor_analysis.ipynb` 作为模板。
