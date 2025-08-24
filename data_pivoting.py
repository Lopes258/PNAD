
import pandas as pd
import logging

logger = logging.getLogger(__name__)
MAX_COLUMN_LENGTH = 128  # Limite do SQL Server

def pivot_sidra_data(df):
    if df is None or df.empty:
        logger.warning("DataFrame vazio para pivotar.")
        return None

    required_cols = ["V", "D1N", "D2N", "D3N"]
    if not all(col in df.columns for col in required_cols):
        logger.error(f"Colunas necessárias para pivoteamento ausentes: {set(required_cols) - set(df.columns)}")
        return None

    try:
        df["V"] = pd.to_numeric(df["V"], errors='coerce')
        df_cleaned = df.dropna(subset=["V"]).copy()
        if df_cleaned.empty:
            logger.warning("Nenhum dado válido para pivotar após limpeza.")
            return None

        df_pivot = df_cleaned.pivot_table(
            index=["D2N", "D3N", "Tabela_ID", "Data_Extracao", "Nivel_Geografico"],
            columns="D1N",
            values="V",
            aggfunc='first'
        ).reset_index()

        # Limpeza e padronização dos nomes das colunas
        new_columns = []
        counts = {}
        for col in df_pivot.columns:
            col_name = str(col).strip().lower()
            col_name = col_name.replace(" ", "_").replace(".", "").replace("-", "_").replace("(", "").replace(")", "").replace("/", "_")

            # Prefixo se começar com número
            if col_name and col_name[0].isdigit():
                col_name = f"col_{col_name}"

            # Truncar para 128 caracteres
            col_name = col_name[:128]

            # Tornar único caso já exista
            original_name = col_name
            i = 1
            while col_name in counts:
                i += 1
                # Truncar novamente para garantir limite
                col_name = f"{original_name[:128 - len(str(i)) - 1]}_{i}"
            counts[col_name] = 1

            new_columns.append(col_name)

        df_pivot.columns = new_columns
        logger.info(f"Dados pivotados com sucesso: {len(df_pivot)} registros e {len(df_pivot.columns)} colunas.")
        return df_pivot

    except Exception as e:
        logger.error(f"Erro ao pivotar dados: {e}")
        return None

