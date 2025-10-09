import pandas as pd
import sys

if len(sys.argv) != 3:
    print("Uso: python3 verificar.py <archivo1.csv> <archivo2.csv>")
    sys.exit(1)

archivo1 = sys.argv[1]
archivo2 = sys.argv[2]

df1 = pd.read_csv(archivo1)
df2 = pd.read_csv(archivo2)

# Merge y verificar
resultado = df1.merge(df2, how='left', indicator=True)
todas = (resultado['_merge'] == 'both').all()

print("TODAS en archivo 2" if todas else "FALTAN algunas")
