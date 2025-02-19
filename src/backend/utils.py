# %%
import sqlite3


# %%
def get_table_attributes(db_file, table_name):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]

    conn.close()
    return columns


# %%


def calculate_stock_value(db_file, table_name):

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    query = f"""
            SELECT
                `Part Number`,
                `Part standard price`,
                `Stock qty`,
                `Part standard price` * `Stock qty` AS StockValue
            FROM
                `{table_name}`
        """

    cursor.execute(query)
    rows = cursor.fetchall()

    print("Stock Value Calculation:")
    print("-" * 30)
    for row in rows:
        part_number = row[0]
        price = row[1]
        quantity = row[2]
        stock_value = row[3]
        print(
            f"Part Number: {part_number}, Price: {price}, Quantity: {quantity}, Stock Value: {stock_value:.2f}")

    conn.close()


# %%
def add_stock_value_column(db_file, table_name):
    """
    Adds a 'Stock Value' column to the specified table in the SQLite database
    and calculates the stock value for each row, storing the result in the new column.

    Args:
        db_file (str): Path to the SQLite database file.
        table_name (str): Name of the table.
    """
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `Stock Value` REAL")

    update_query = f"""
        UPDATE `{table_name}`
        SET `Stock Value` = `Part standard price` * `Stock qty`
    """
    cursor.execute(update_query)
    conn.commit()
    conn.close()


# %%

def add_days_column(db_file, table_name):

    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Check if 'Days' column already exists
        cursor.execute(f"PRAGMA table_info(`{table_name}`)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'Days' in columns:
            print(f"Column 'Days' already exists in table '{table_name}'.")
        else:
            # Add the 'Days' column (REAL for potential decimal values)
            cursor.execute(
                f"ALTER TABLE `{table_name}` ADD COLUMN `Days` REAL")
            print(f"Added 'Days' column to table '{table_name}'.")

        # Update the 'Days' column based on the logic
        update_query = f"""
            UPDATE `{table_name}`
            SET `Days` =
                CASE
                    WHEN `Yearly consumption` = 0 THEN 0
                    ELSE ROUND(CAST(CAST(`Stock qty` AS REAL) / `Yearly consumption` * 365 AS REAL),0)
                END
        """
        cursor.execute(update_query)
        conn.commit()
        print(
            f"Calculated and updated 'Days' for all rows in table '{table_name}'.")

        conn.close()

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


# %%
def add_Safety_Stock_impact_column(db_file, table_name):

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute(f"PRAGMA table_info(`{table_name}`)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'Safety Stock impact' in columns:
        print(
            f"Column 'Safety Stock impact' already exists in table '{table_name}'.")
    else:
        # Add the 'Safety Stock impact' column (REAL for potential decimal values)
        cursor.execute(
            f"ALTER TABLE `{table_name}` ADD COLUMN `Safety Stock impact` REAL")
        print(f"Added 'Safety Stock impact' column to table '{table_name}'.")

    update_query = f"""
        UPDATE `{table_name}`
        SET `Safety Stock impact` = ROUND(`Part standard price` * `Safety Stock`,0)
    """
    cursor.execute(update_query)
    conn.commit()
    conn.close()


# %%
def add_Pending_Order_value_column(db_file, table_name):

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute(f"PRAGMA table_info(`{table_name}`)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'Pending Order value' in columns:
        print(
            f"Column 'Pending Order value' already exists in table '{table_name}'.")
    else:
        # Add the 'Pending Order value' column (REAL for potential decimal values)
        cursor.execute(
            f"ALTER TABLE `{table_name}` ADD COLUMN `Pending Order value` REAL")
        print(f"Added 'Pending Order value' column to table '{table_name}'.")

    update_query = f"""
        UPDATE `{table_name}`
        SET `Pending Order value` = ROUND(`Part standard price` * `Pending orders`,0)
    """
    cursor.execute(update_query)
    conn.commit()
    conn.close()


# %%
def add_Consumption_value_column(db_file, table_name):

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute(f"PRAGMA table_info(`{table_name}`)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'Consumption value' in columns:
        print(
            f"Column 'Consumption value' already exists in table '{table_name}'.")
    else:
        # Add the 'Consumption value' column (REAL for potential decimal values)
        cursor.execute(
            f"ALTER TABLE `{table_name}` ADD COLUMN `Consumption value` REAL")
        print(f"Added 'Consumption value' column to table '{table_name}'.")

    update_query = f"""
        UPDATE `{table_name}`
        SET `Consumption value` = ROUND(`Part standard price` * `Yearly consumption`,0)
    """
    cursor.execute(update_query)
    conn.commit()
    conn.close()


# %%
def add_Coverage_class_column(db_file, table_name):

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute(f"PRAGMA table_info(`{table_name}`)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'Coverage class' in columns:
        print(
            f"Column 'Coverage class' already exists in table '{table_name}'.")
    else:
        # Add the 'Coverage class' column (REAL for potential decimal values)
        cursor.execute(
            f"ALTER TABLE `{table_name}` ADD COLUMN `Coverage class` TEXT")
        print(f"Added 'Coverage class' column to table '{table_name}'.")

    update_query = f"""
            UPDATE `{table_name}`
            SET `Coverage class` =
                CASE
                    WHEN `Order specific part` = 'Order specific' THEN `Order specific part`
                    WHEN `Days` IS NULL OR `Days` = '' THEN 'Not moving'
                    WHEN `Days` < 15 THEN '<15 days'
                    WHEN `Days` < 30 THEN '<30 days'
                    WHEN `Days` < 90 THEN '<90 days'
                    ELSE '>90 days'
                END
        """
    cursor.execute(update_query)
    conn.commit()
    conn.close()


# %%
def add_Consumption_class_column(db_file, table_name):
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        cursor.execute(f"PRAGMA table_info(`{table_name}`)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'Consumption class' in columns:
            print(
                f"Column 'Consumption class' already exists in table '{table_name}'.")
        else:

            cursor.execute(
                f"ALTER TABLE `{table_name}` ADD COLUMN `Consumption class` TEXT")
            print(f"Added 'Consumption class' column to table '{table_name}'.")

        # Calculate percentiles using a different method
        # First, get all consumption values in sorted order
        cursor.execute(f"""
            SELECT `Consumption value`
            FROM `{table_name}`
            WHERE `Consumption value` IS NOT NULL
            ORDER BY `Consumption value`
        """)
        values = [row[0] for row in cursor.fetchall()]

        if values:
            # Calculate 90th and 60th percentiles
            percentile_90_idx = int(len(values) * 0.90)
            percentile_60_idx = int(len(values) * 0.60)
            percentile_90 = values[percentile_90_idx]
            percentile_60 = values[percentile_60_idx]
        else:
            percentile_90 = 0
            percentile_60 = 0

        # Update the Consumption class
        update_query = f"""
            UPDATE `{table_name}`
            SET `Consumption class` =
                CASE
                    WHEN `Order specific part` = 'Order specific' THEN 'Order specific'
                    WHEN `Consumption value` IS NULL OR `Consumption value` = 0 THEN '4'
                    WHEN `Consumption value` > {percentile_90} THEN '1'
                    WHEN `Consumption value` > {percentile_60} THEN '2'
                    ELSE '3'
                END
        """

        cursor.execute(update_query)
        conn.commit()
        print(
            f"Calculated and updated 'Consumption class' for all rows in table '{table_name}'.")

        conn.close()

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


# %%


def add_stock_class_column(db_file, table_name):

    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Check if 'Stock Class' column already exists
        cursor.execute(f"PRAGMA table_info(`{table_name}`)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'Stock Class' in columns:
            print(
                f"Column 'Stock Class' already exists in table '{table_name}'.")
        else:
            # Add the 'Stock Class' column (TEXT for string values: A, B, C, D)
            cursor.execute(
                f"ALTER TABLE `{table_name}` ADD COLUMN `Stock Class` TEXT")
            print(f"Added 'Stock Class' column to table '{table_name}'.")

        # Calculate percentiles using the index-based method
        cursor.execute(f"""
            SELECT `Stock Value`
            FROM `{table_name}`
            WHERE `Stock Value` IS NOT NULL
            ORDER BY `Stock Value`
        """)
        values = [row[0] for row in cursor.fetchall()]

        if values:
            # Calculate 90th and 60th percentiles indices
            percentile_90_idx = int(len(values) * 0.90)
            percentile_60_idx = int(len(values) * 0.60)

            # Handle edge cases if the indices fall at the very end
            percentile_90_idx = min(percentile_90_idx, len(
                values) - 1)  # Ensure index is within range
            percentile_60_idx = min(percentile_60_idx, len(
                values) - 1)  # Ensure index is within range

            percentile_90 = values[percentile_90_idx]
            percentile_60 = values[percentile_60_idx]
        else:
            percentile_90 = 0  # Set to 0 if there's no data
            percentile_60 = 0  # Set to 0 if there's no data

        # Update the 'Stock Class' column
        update_query = f"""
            UPDATE `{table_name}`
            SET `Stock Class` =
                CASE
                    WHEN `Stock Value` IS NULL OR `Stock Value` = 0 THEN 'D'
                    WHEN `Stock Value` > {percentile_90} THEN 'A'
                    WHEN `Stock Value` > {percentile_60} THEN 'B'
                    ELSE 'C'
                END
        """

        cursor.execute(update_query)
        conn.commit()
        print(
            f"Calculated and updated 'Stock Class' for all rows in table '{table_name}'.")

        conn.close()

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


# %%


def add_days_objective_column(db_file, table_name):

    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        cursor.execute(f"PRAGMA table_info(`{table_name}`)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'Days Objective' in columns:
            print(
                f"Column 'Days Objective' already exists in table '{table_name}'.")
        else:

            cursor.execute(
                f"ALTER TABLE `{table_name}` ADD COLUMN `Days Objective` REAL")
            print(f"Added 'Days Objective' column to table '{table_name}'.")

        # Update the 'Days Objective' column
        update_query = f"""
            UPDATE `{table_name}`
            SET `Days Objective` =
                CASE
                    WHEN `Order specific part` = 'Order specific' THEN NULL
                    WHEN `Consumption Class` = 4 THEN 0
                    ELSE (`Consumption Class` * 7) + (CASE WHEN `Location` = 'Imported' THEN 14 ELSE 0 END)
                END
        """

        cursor.execute(update_query)
        conn.commit()
        print(
            f"Calculated and updated 'Days Objective' for all rows in table '{table_name}'.")

        conn.close()

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


# %%


def add_benefit_column(db_file, table_name):

    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Check if 'Benefit' column already exists
        cursor.execute(f"PRAGMA table_info(`{table_name}`)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'Benefit' in columns:
            print(f"Column 'Benefit' already exists in table '{table_name}'.")
        else:
            # Add the 'Benefit' column (REAL for decimal values)
            cursor.execute(
                f"ALTER TABLE `{table_name}` ADD COLUMN `Benefit` REAL")
            print(f"Added 'Benefit' column to table '{table_name}'.")

        # Update the 'Benefit' column
        update_query = f"""
            UPDATE `{table_name}`
            SET `Benefit` =
                CASE
                    WHEN `Days Objective` IS NULL OR `Days Objective` = '' THEN 0
                    WHEN `Days Objective` = 0 THEN round(`Stock Value` * 0.66, 0)  
                    ELSE round(
                        (
                            `Stock Value` -
                            CASE
                                WHEN `Days` > `Days Objective` THEN round((`Stock Value` / CAST(`Days` AS REAL)) * `Days Objective`, 0)
                                ELSE 0
                            END
                        ) * 0.66, 0) 
                END
        """

        cursor.execute(update_query)
        conn.commit()
        print(
            f"Calculated and updated 'Benefit' for all rows in table '{table_name}'.")

        conn.close()

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# %%


def add_Batch_Size_impact_column(db_file, table_name):

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute(f"PRAGMA table_info(`{table_name}`)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'Batch size impact' in columns:
        print(
            f"Column 'Batch size impact' already exists in table '{table_name}'.")
    else:
        # Add the 'Batch size impact' column (REAL for potential decimal values)
        cursor.execute(
            f"ALTER TABLE `{table_name}` ADD COLUMN `Batch size impact` REAL")
        print(f"Added 'Batch size impact' column to table '{table_name}'.")

    update_query = f"""
        UPDATE `{table_name}`
        SET `Batch size impact` = ROUND(`Part standard price` * `Batch size`,0)
    """
    cursor.execute(update_query)
    conn.commit()
    conn.close()
