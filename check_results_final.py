#!/usr/bin/env python3
"""
Script para comparar reportes generados por el sistema distribuido
contra reportes de referencia (ground truth).
"""

import os
import sys
import csv
from typing import List, Dict, Tuple, Set
from dataclasses import dataclass
from collections import defaultdict

@dataclass
class ComparisonResult:
    query_name: str
    total_expected: int
    total_generated: int
    matching_rows: int
    missing_rows: int
    extra_rows: int
    different_values: List[Dict]
    is_exact_match: bool

def read_csv_as_set(filepath: str) -> Tuple[List[str], Set[str]]:
    """Lee CSV y retorna (header, set de líneas normalizadas)."""
    if not os.path.exists(filepath):
        return [], set()
    
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    if not lines:
        return [], set()
    
    header = lines[0].strip()
    data_lines = set()
    
    for line in lines[1:]:
        line = line.strip()
        if line:
            # Normalizar: quitar espacios extras
            data_lines.add(line)
    
    return [header], data_lines

def read_csv_as_list(filepath: str) -> Tuple[List[str], List[Dict]]:
    """Lee CSV y retorna (headers, lista de registros como dict)."""
    if not os.path.exists(filepath):
        return [], []
    
    records = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        for row in reader:
            records.append(row)
    
    return headers, records

def compare_reports(expected_path: str, generated_path: str, query_name: str) -> ComparisonResult:
    """Compara dos archivos CSV línea por línea."""
    
    # Leer como conjuntos para comparación rápida
    exp_header, expected_lines = read_csv_as_set(expected_path)
    gen_header, generated_lines = read_csv_as_set(generated_path)
    
    # Encontrar diferencias
    missing = expected_lines - generated_lines
    extra = generated_lines - expected_lines
    matching = expected_lines & generated_lines
    
    # Analizar diferencias de valores (si hay missing/extra, buscar coincidencias parciales)
    different_values = []
    if missing or extra:
        # Leer como dict para análisis detallado
        _, exp_records = read_csv_as_list(expected_path)
        _, gen_records = read_csv_as_list(generated_path)
        
        # Crear índice por primera columna (usualmente ID)
        if exp_records and gen_records:
            first_col = list(exp_records[0].keys())[0] if exp_records[0] else None
            if first_col:
                exp_by_id = {r[first_col]: r for r in exp_records}
                gen_by_id = {r[first_col]: r for r in gen_records}
                
                # Buscar registros con mismo ID pero valores diferentes
                for key in exp_by_id.keys() & gen_by_id.keys():
                    exp_rec = exp_by_id[key]
                    gen_rec = gen_by_id[key]
                    
                    if exp_rec != gen_rec:
                        diff = {
                            'id': key,
                            'expected': exp_rec,
                            'generated': gen_rec
                        }
                        different_values.append(diff)
    
    is_exact_match = (len(missing) == 0 and len(extra) == 0 and 
                     exp_header == gen_header)
    
    return ComparisonResult(
        query_name=query_name,
        total_expected=len(expected_lines),
        total_generated=len(generated_lines),
        matching_rows=len(matching),
        missing_rows=len(missing),
        extra_rows=len(extra),
        different_values=different_values,
        is_exact_match=is_exact_match
    )

def print_comparison_summary(result: ComparisonResult):
    """Imprime resumen de comparación."""
    print(f"\n{'='*60}")
    print(f"Query: {result.query_name}")
    print(f"{'='*60}")
    
    if result.is_exact_match:
        print(f"✅ EXACTO: Los reportes son idénticos")
        print(f"   Total registros: {result.total_expected}")
    else:
        print(f"❌ DIFERENCIAS ENCONTRADAS:")
        print(f"   Registros esperados:  {result.total_expected}")
        print(f"   Registros generados:  {result.total_generated}")
        print(f"   Coincidencias:        {result.matching_rows}")
        print(f"   Faltantes:            {result.missing_rows}")
        print(f"   Extras:               {result.extra_rows}")
        
        if result.different_values:
            print(f"   Valores diferentes:   {len(result.different_values)}")

def print_detailed_differences(result: ComparisonResult, expected_path: str, 
                               generated_path: str, max_show: int = 10):
    """Imprime diferencias detalladas."""
    if result.is_exact_match:
        return
    
    print(f"\n--- Diferencias detalladas para {result.query_name} ---")
    
    # Mostrar registros faltantes
    if result.missing_rows > 0:
        print(f"\n🔴 Registros FALTANTES (primeros {max_show}):")
        _, expected_lines = read_csv_as_set(expected_path)
        _, generated_lines = read_csv_as_set(generated_path)
        missing = expected_lines - generated_lines
        
        for i, line in enumerate(sorted(missing)[:max_show]):
            print(f"   {i+1}. {line}")
        
        if len(missing) > max_show:
            print(f"   ... y {len(missing) - max_show} más")
    
    # Mostrar registros extras
    if result.extra_rows > 0:
        print(f"\n🟡 Registros EXTRAS (primeros {max_show}):")
        _, expected_lines = read_csv_as_set(expected_path)
        _, generated_lines = read_csv_as_set(generated_path)
        extra = generated_lines - expected_lines
        
        for i, line in enumerate(sorted(extra)[:max_show]):
            print(f"   {i+1}. {line}")
        
        if len(extra) > max_show:
            print(f"   ... y {len(extra) - max_show} más")
    
    # Mostrar valores diferentes
    if result.different_values:
        print(f"\n🔵 Valores DIFERENTES (primeros {max_show}):")
        for i, diff in enumerate(result.different_values[:max_show]):
            print(f"   {i+1}. ID: {diff['id']}")
            print(f"      Esperado: {diff['expected']}")
            print(f"      Generado: {diff['generated']}")
        
        if len(result.different_values) > max_show:
            print(f"   ... y {len(result.different_values) - max_show} más")

def main():
    # Determinar cliente
    client_id = sys.argv[1] if len(sys.argv) > 1 else '0'
    
    # Directorios
    reference_dir = './reports/test'
    generated_dir = f'./report_client_{client_id}'
    
    # Verificar que existan los directorios
    if not os.path.exists(reference_dir):
        print(f"❌ Error: No existe el directorio de referencia: {reference_dir}")
        sys.exit(1)
    
    if not os.path.exists(generated_dir):
        print(f"❌ Error: No existe el directorio de reportes generados: {generated_dir}")
        sys.exit(1)
    
    print(f"\n{'='*60}")
    print(f"COMPARACIÓN DE REPORTES - Cliente {client_id}")
    print(f"{'='*60}")
    print(f"Referencia: {reference_dir}")
    print(f"Generados:  {generated_dir}")
    
    # Mapeo de archivos (referencia -> generado)
    comparisons = [
        ('query1', 'generated_query1.csv', 'report_q1.csv'),
        ('query2a', 'generated_query2a.csv', 'report_q2_best_selling.csv'),
        ('query2b', 'generated_query2b.csv', 'report_q2_most_profit.csv'),
        ('query3', 'generated_query3.csv', 'report_q3.csv'),
        ('query4', 'generated_query4.csv', 'report_q4.csv'),
    ]
    
    results = []
    
    for query_name, ref_file, gen_file in comparisons:
        expected_path = os.path.join(reference_dir, ref_file)
        generated_path = os.path.join(generated_dir, gen_file)
        
        if not os.path.exists(expected_path):
            print(f"\n⚠️  No se encontró archivo de referencia: {ref_file}")
            continue
        
        if not os.path.exists(generated_path):
            print(f"\n⚠️  No se encontró archivo generado: {gen_file}")
            continue
        
        # Comparar
        result = compare_reports(expected_path, generated_path, query_name)
        results.append(result)
        
        # Mostrar resumen
        print_comparison_summary(result)
    
    # Resumen final
    print(f"\n{'='*60}")
    print("RESUMEN FINAL")
    print(f"{'='*60}")
    
    total_queries = len(results)
    exact_matches = sum(1 for r in results if r.is_exact_match)
    
    print(f"Queries comparadas: {total_queries}")
    print(f"Coincidencias exactas: {exact_matches}")
    print(f"Con diferencias: {total_queries - exact_matches}")
    
    if exact_matches == total_queries and total_queries > 0:
        print(f"\n🎉 ¡TODOS LOS REPORTES COINCIDEN PERFECTAMENTE!")
    else:
        print(f"\n⚠️  Hay diferencias en algunos reportes")
        
        # Mostrar detalles de diferencias
        print(f"\n¿Mostrar diferencias detalladas? (y/n): ", end='')
        try:
            response = input().strip().lower()
        except EOFError:
            response = 'n'
        
        if response == 'y':
            for result in results:
                if not result.is_exact_match:
                    # Reconstruir paths
                    ref_file = {
                        'query1': 'generated_query1.csv',
                        'query2a': 'generated_query2a.csv',
                        'query2b': 'generated_query2b.csv',
                        'query3': 'generated_query3.csv',
                        'query4': 'generated_query4.csv'
                    }.get(result.query_name)
                    
                    gen_file = {
                        'query1': 'report_q1.csv',
                        'query2a': 'report_q2_best_selling.csv',
                        'query2b': 'report_q2_most_profit.csv',
                        'query3': 'report_q3.csv',
                        'query4': 'report_q4.csv'
                    }.get(result.query_name)
                    
                    if ref_file and gen_file:
                        expected_path = os.path.join(reference_dir, ref_file)
                        generated_path = os.path.join(generated_dir, gen_file)
                        print_detailed_differences(result, expected_path, generated_path)
    
    # Exit code
    sys.exit(0 if exact_matches == total_queries else 1)

if __name__ == "__main__":
    main()