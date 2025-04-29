import re
import pandas as pd
import psycopg2
from psycopg2 import sql

DB_CONFIG = {
    "host": "localhost",
    "database": "minha_base",
    "user": "postgres",
    "password": "admin",
}

# Mapeamento de estados por extenso → sigla de 2 letras
UF_MAP = {
    'Acre': 'AC', 'Alagoas': 'AL', 'Amapá': 'AP', 'Amazonas': 'AM',
    'Bahia': 'BA', 'Ceará': 'CE', 'Distrito Federal': 'DF',
    'Espírito Santo': 'ES', 'Goiás': 'GO', 'Maranhão': 'MA',
    'Mato Grosso': 'MT', 'Mato Grosso do Sul': 'MS', 'Minas Gerais': 'MG',
    'Pará': 'PA', 'Paraíba': 'PB', 'Paraná': 'PR', 'Pernambuco': 'PE',
    'Piauí': 'PI', 'Rio de Janeiro': 'RJ', 'Rio Grande do Norte': 'RN',
    'Rio Grande do Sul': 'RS', 'Rondônia': 'RO', 'Roraima': 'RR',
    'Santa Catarina': 'SC', 'São Paulo': 'SP', 'Sergipe': 'SE',
    'Tocantins': 'TO'
}

def carregar_mapeamento(cursor, tabela, coluna_descricao):
    cursor.execute(
        sql.SQL("SELECT id, {} FROM {}").format(
            sql.Identifier(coluna_descricao),
            sql.Identifier(tabela)
        )
    )
    return {descricao: id for id, descricao in cursor.fetchall()}

def main():
    # 1) lê o Excel
    try:
        df = pd.read_excel("dados_importacao.xlsx")
        print("✅ Planilha lida com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao ler o Excel: {e}")
        return

    # 2) conecta ao PostgreSQL
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()
        print("✅ Conectado ao PostgreSQL!")
    except Exception as e:
        print(f"❌ Erro de conexão: {e}")
        return

    erros = []
    imported_clientes = 0
    imported_contratos = 0

    # 3) carrega mapeamentos
    try:
        planos_map        = carregar_mapeamento(cursor, "tbl_planos",          "descricao")
        status_map        = carregar_mapeamento(cursor, "tbl_status_contrato", "status")
        tipos_contato_map = carregar_mapeamento(cursor, "tbl_tipos_contato",   "tipo_contato")
    except psycopg2.Error as e:
        print(f"❌ Erro ao carregar mapeamentos: {e}")
        conn.close()
        return

    # 4) carrega clientes já existentes
    try:
        cursor.execute("SELECT cpf_cnpj, id FROM tbl_clientes")
        cpf_cnpj_to_id = {cpf: cid for cpf, cid in cursor.fetchall()}
    except psycopg2.Error as e:
        print(f"❌ Erro ao carregar clientes existentes: {e}")
        conn.close()
        return

    # mapeamento das colunas do DF para rótulos em tbl_tipos_contato
    DF_to_DB_tipos = {
        'Celulares':  'Celular',
        'Telefones':  'Telefone',
        'Emails':     'E-Mail'
    }

    # 5) loop de importação
    for idx, row in df.iterrows():
        linha_excel = idx + 2
        try:
            # — CPF/CNPJ —
            cpf_cnpj = (
                str(row['CPF/CNPJ'])
                .replace(".", "")
                .replace("-", "")
                .replace("/", "")
                .strip()
            )
            if not cpf_cnpj.isdigit() or len(cpf_cnpj) not in (11, 14):
                raise ValueError(f"CPF/CNPJ inválido: '{cpf_cnpj}' (esperado 11 ou 14 dígitos)")

            if cpf_cnpj in cpf_cnpj_to_id:
                cliente_id = cpf_cnpj_to_id[cpf_cnpj]
            else:
                data_nasc = (
                    pd.to_datetime(row['Data Nasc.'], errors='coerce').date()
                    if not pd.isna(row['Data Nasc.']) else None
                )
                data_cad = (
                    pd.to_datetime(row['Data Cadastro cliente'], errors='coerce')
                    if not pd.isna(row['Data Cadastro cliente']) else None
                )
                nome_fantasia = row['Nome Fantasia'] if not pd.isna(row['Nome Fantasia']) else None

                cursor.execute(
                    """
                    INSERT INTO tbl_clientes
                      (nome_razao_social, nome_fantasia, cpf_cnpj, data_nascimento, data_cadastro)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        row['Nome/Razão Social'],
                        nome_fantasia,
                        cpf_cnpj,
                        data_nasc,
                        data_cad
                    )
                )
                cliente_id = cursor.fetchone()[0]
                cpf_cnpj_to_id[cpf_cnpj] = cliente_id
                imported_clientes += 1

# — contatos: Celulares, Telefones, Emails —
            for coluna_df, rotulo_db in DF_to_DB_tipos.items():
                raw = row[coluna_df]
                if pd.isna(raw):
                    continue

                if isinstance(raw, float) and raw.is_integer():
                    raw = int(raw)

                raw_str = str(raw).strip()
                for parte in re.split(r'[;,]', raw_str):
                    contato = parte.strip()
                    if contato:
                        tipo_id = tipos_contato_map[rotulo_db]
                        cursor.execute(
                            """
                            INSERT INTO tbl_cliente_contatos
                            (cliente_id, tipo_contato_id, contato)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (cliente_id, tipo_contato_id, contato) DO NOTHING
                            """,
                            (cliente_id, tipo_id, contato)
                        )


            # — plano & status —
            plano  = row['Plano']
            status = row['Status']

            if plano not in planos_map:
                cursor.execute(
                    sql.SQL("INSERT INTO tbl_planos (descricao, valor) VALUES (%s, %s) RETURNING id"),
                    (plano, row['Plano Valor'])
                )
                planos_map[plano] = cursor.fetchone()[0]

            if status not in status_map:
                cursor.execute(
                    sql.SQL("INSERT INTO tbl_status_contrato (status) VALUES (%s) RETURNING id"),
                    (status,)
                )
                status_map[status] = cursor.fetchone()[0]

            # — valida campos obrigatórios —
            for f in ['Vencimento', 'CEP', 'UF']:
                if pd.isna(row[f]):
                    raise ValueError(f"Campo obrigatório '{f}' ausente")

            # — UF: converte nome para sigla —
            uf_raw = str(row['UF']).strip()
            uf = UF_MAP.get(uf_raw)
            if not uf:
                raise ValueError(f"UF desconhecida na linha {linha_excel}: '{uf_raw}'")

            # — isento —
            isento = not pd.isna(row['Isento'])

            # — insere contrato —
            cursor.execute(
                """
                INSERT INTO tbl_cliente_contratos
                  (cliente_id, plano_id, dia_vencimento, isento,
                   endereco_logradouro, endereco_numero, endereco_bairro,
                   endereco_cidade, endereco_complemento, endereco_cep,
                   endereco_uf, status_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    cliente_id,
                    planos_map[plano],
                    row['Vencimento'],
                    isento,
                    row['Endereço'],
                    row['Número'],
                    row['Bairro'],
                    row['Cidade'],
                    row['Complemento'],
                    str(row['CEP']).replace("-", "").strip(),
                    uf,
                    status_map[status]
                )
            )
            imported_contratos += 1
            conn.commit()

        except psycopg2.DatabaseError as e:
            conn.rollback()
            erros.append({"linha": linha_excel, "motivo": f"Erro no PostgreSQL: {e}"})
        except ValueError as e:
            conn.rollback()
            erros.append({"linha": linha_excel, "motivo": str(e)})
        except Exception as e:
            conn.rollback()
            erros.append({"linha": linha_excel, "motivo": f"Erro desconhecido: {e}"})

    cursor.close()
    conn.close()

    # 6) resumo final
    print("\n📊 Resumo Final:")
    print(f"Total de registros: {len(df)}")
    print(f"Clientes importados: {imported_clientes}")
    print(f"Contratos importados: {imported_contratos}")
    print(f"Erros: {len(erros)}")
    if erros:
        print("\n❌ Detalhes dos erros:")
        for e in erros:
            print(f"Linha {e['linha']}: {e['motivo']}")

if __name__ == "__main__":
    main()
