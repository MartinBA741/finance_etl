cd Documents/code/finance_etl/redshift
python A_create_cluster.py
# Change Redshift properties / security Group
python B_check_cluster.py
python C_connect_to_cluster.py
python D_create_tables.py 
python E_etl.py
python F_inspect.py

python Z_kill_cluster.py