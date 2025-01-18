import pandas as pd
import os
import re

def validate_email(email):
    """Valida el formato del email."""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, str(email).strip().lower()))

def process_contacts(input_file, batch_size=500):
    """
    Procesa el archivo de contactos:
    1. Lee el archivo
    2. Limpia y valida emails
    3. Elimina duplicados
    4. Divide en lotes
    """
    # Leer el archivo
    df = pd.read_excel('archivopadre.xlsx', skiprows=2)
    print(f"Registros originales: {len(df)}")

    # Renombrar columnas y separar nombre/apellido
    df.columns = ['NombreCompleto', 'Email']
    df[['Apellido', 'Nombre']] = df['NombreCompleto'].str.split(',', expand=True)

    # Limpiar datos
    df['Email'] = df['Email'].str.strip().str.lower()
    df['Apellido'] = df['Apellido'].str.strip().str.title()
    df['Nombre'] = df['Nombre'].str.strip().str.title()

    # Validar emails
    valid_emails_mask = df['Email'].apply(validate_email)
    invalid_emails = df[~valid_emails_mask]
    if not invalid_emails.empty:
        print("\nEmails inválidos encontrados:")
        print(invalid_emails[['Email', 'Apellido', 'Nombre']])
        df = df[valid_emails_mask]

    # Eliminar duplicados
    duplicates = df[df.duplicated(subset=['Email'], keep='first')]
    if not duplicates.empty:
        print("\nEmails duplicados encontrados (se mantendrá la primera ocurrencia):")
        print(duplicates[['Email', 'Apellido', 'Nombre']])
    
    df = df.drop_duplicates(subset=['Email'], keep='first')

    # Reordenar columnas y eliminar columna original de nombre completo
    df = df[['Apellido', 'Nombre', 'Email']]

    print(f"\nRegistros después de limpieza: {len(df)}")

    # Dividir en lotes
    num_batches = (len(df) + batch_size - 1) // batch_size
    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, len(df))
        batch_df = df[start_idx:end_idx]
        
        # Crear directorio para los lotes si no existe
        if not os.path.exists('lotes'):
            os.makedirs('lotes')
            
        output_file = f'lotes/contactos_lote_{i+1}.csv'
        batch_df.to_csv(output_file, index=False)
        print(f"Lote {i+1} guardado: {output_file} ({len(batch_df)} registros)")

    return len(df)

if __name__ == "__main__":
    # Reemplaza 'archivopadre.xlsx' con el nombre de tu archivo
    input_file = 'archivopadre.xlsx'
    total_processed = process_contacts(input_file)
    print(f"\nProceso completado. Total de registros válidos: {total_processed}")