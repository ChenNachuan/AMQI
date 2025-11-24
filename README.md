# AMQI 2025 项目

## 项目描述

本项目是 2025 年资产管理与量化投资课程的大作业。目标是利用中国股票市场数据，构建基于市场异象的交易策略。策略的表现将对照经典的因子模型进行评估。

## 数据字典

本项目使用中国股票市场数据，包含以下变量：

| 变量 | 描述 |
| :--- | :--- |
| `stkcd` | 中国 A 股股票代码 |
| `year`, `month` | 股票收益记录的年份和月份 |
| `ret` | 月度股票超额收益 |
| `size` | 市值（千元人民币） |
| `r11` | 12-2 个月动量 |
| `bm` | 账面市值比 |
| `ep` | 市盈率（盈利/价格） |
| `roe` | 净资产收益率 |
| `ivff` | 基于 FF 3 因子模型的特质波动率 |
| `beta` | CAPM beta |
| `tur` | 月度换手率 |
| `srev` | 短期反转（t-1 月收益率） |

## 设置说明

1. **环境设置**:
    确保已安装 Python。建议使用虚拟环境。

    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

2. **数据处理**:
    原始数据为 Stata 格式 (`.dta`)。运行转换脚本以生成优化的 Parquet 文件：

    ```bash
    python convert_data.py
    ```

## 使用方法

在分析脚本中加载处理后的数据：

```python
from data_loader import load_stock_data

df = load_stock_data()
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
