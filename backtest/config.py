import yaml
import os

def load_config():
    """
    Load global configuration from config.yaml in the project root.
    """
    # Find project root (assuming this file is in backtest/ or scripts/utils/)
    # Actually, let's look relative to this file.
    # If this file is in backtest/config.py, root is ../
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Assuming this file is placed in AMQI/backtest/
    project_root = os.path.dirname(current_dir) 
    
    config_path = os.path.join(project_root, 'config.yaml')
    
    if not os.path.exists(config_path):
        # Fallback defaults
        return {
            'backtest': {
                'start_date': '2010-01-01',
                'end_date': '2025-12-31'
            }
        }
        
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
        
    return config
