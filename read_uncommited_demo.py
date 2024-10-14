import mysql.connector
import os
from dotenv import load_dotenv


load_dotenv()


connection = mysql.connector.connect(
    host=os.getenv('DB_HOST'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    database=os.getenv('DB_NAME')
)


def dirty_read_demo():
    cursor1 = connection.cursor()
    cursor2 = connection.cursor()

    # Встановлюємо рівень ізоляції на READ UNCOMMITTED для першої транзакції
    cursor1.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
    cursor1.execute("START TRANSACTION")
    cursor1.execute("UPDATE accounts SET balance = balance + 100 WHERE name = 'Alice'")
    print("Транзакція 1: змінила баланс Alice (без коміту)")

    # Друга транзакція зчитує ті ж дані
    cursor2.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
    result = cursor2.fetchone()
    print(f"Транзакція 2: читає баланс Alice: {result[0]} (брудне читання)")

    # Закриваємо невичитані результати перед ROLLBACK
    cursor1.fetchall()

    # Відкочуємо першу транзакцію (не зберігаємо зміни)
    cursor1.execute("ROLLBACK")
    print("Транзакція 1: зроблено ROLLBACK")

    cursor1.close()
    cursor2.close()



def read_committed_demo():
    cursor1 = connection.cursor()
    cursor2 = connection.cursor()

    # Встановлюємо рівень ізоляції на READ COMMITTED для обох транзакцій перед їх початком
    cursor1.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
    cursor2.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")

    # Починаємо транзакцію 1
    cursor1.execute("START TRANSACTION")
    cursor1.execute("UPDATE accounts SET balance = balance + 50 WHERE name = 'Bob'")
    print("Транзакція 1: змінила баланс Bob (без коміту)")

    # Транзакція 2 читає значення після модифікації, але не бачить незакомічених змін
    cursor2.execute("START TRANSACTION")
    cursor2.execute("SELECT balance FROM accounts WHERE name = 'Bob'")
    result = cursor2.fetchone()
    print(f"Транзакція 2: читає баланс Bob (немає брудного читання): {result[0]}")

    # Закриваємо невичитані результати перед COMMIT
    cursor1.fetchall()

    # Фіксуємо зміни першої транзакції
    cursor1.execute("COMMIT")
    print("Транзакція 1: зроблено COMMIT")

    cursor1.close()
    cursor2.close()


def repeatable_read_demo():
    cursor1 = connection.cursor()
    cursor2 = connection.cursor()

    cursor1.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    cursor1.execute("START TRANSACTION")
    cursor1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
    result = cursor1.fetchone()
    print(f"Транзакція 1: баланс Alice (перший раз): {result[0]}")

    # Закриваємо курсор або зчитуємо результати перед виконанням наступних запитів
    cursor1.fetchall()  # Додано для уникнення проблеми з unread results

    cursor2.execute("START TRANSACTION")
    cursor2.execute("UPDATE accounts SET balance = balance - 30 WHERE name = 'Alice'")
    cursor2.execute("COMMIT")
    print("Транзакція 2: змінила і закомітила баланс Alice")

    cursor1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
    result = cursor1.fetchone()
    print(f"Транзакція 1: баланс Alice (другий раз): {result[0]} (повторне читання)")

    cursor1.fetchall()  # Додано для уникнення проблеми з невичитаними даними

    cursor1.execute("COMMIT")
    cursor1.close()
    cursor2.close()


def non_repeatable_read_demo():
    cursor1 = connection.cursor()
    cursor2 = connection.cursor()

    # Встановлюємо рівень ізоляції на READ COMMITTED
    cursor1.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
    cursor2.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")

    # Транзакція 1 читає баланс Alice
    cursor1.execute("START TRANSACTION")
    cursor1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
    result = cursor1.fetchone()
    print(f"Транзакція 1: баланс Alice (перше читання): {result[0]}")

    # Зчитуємо всі результати (щоб уникнути помилки)
    cursor1.fetchall()

    # Транзакція 2 змінює баланс Alice і робить коміт
    cursor2.execute("START TRANSACTION")
    cursor2.execute("UPDATE accounts SET balance = balance + 100 WHERE name = 'Alice'")
    cursor2.execute("COMMIT")
    print("Транзакція 2: змінила і закомітила баланс Alice")

    # Транзакція 1 знову читає баланс Alice
    cursor1.execute("SELECT balance FROM accounts WHERE name = 'Alice'")
    result = cursor1.fetchone()
    print(f"Транзакція 1: баланс Alice (друге читання): {result[0]} (non-repeatable read)")

    # Закриваємо невичитані результати, якщо вони є
    cursor1.fetchall()

    cursor1.execute("COMMIT")
    cursor1.close()
    cursor2.close()




def deadlock_demo():
    cursor1 = connection.cursor()
    cursor2 = connection.cursor()

    # Транзакція 1 блокує перший ресурс
    cursor1.execute("START TRANSACTION")
    cursor1.execute("UPDATE accounts SET balance = balance + 50 WHERE name = 'Alice'")

    # Транзакція 2 блокує другий ресурс
    cursor2.execute("START TRANSACTION")
    cursor2.execute("UPDATE accounts SET balance = balance + 50 WHERE name = 'Bob'")

    print("Транзакція 1: заблокувала Alice, Транзакція 2: заблокувала Bob")

    # Тепер транзакція 1 намагається заблокувати Bob
    try:
        cursor1.execute("UPDATE accounts SET balance = balance + 50 WHERE name = 'Bob'")
    except mysql.connector.errors.DatabaseError as e:
        print("Транзакція 1: виникло блокування під час спроби змінити Bob", e)

    # Тепер транзакція 2 намагається заблокувати Alice
    try:
        cursor2.execute("UPDATE accounts SET balance = balance + 50 WHERE name = 'Alice'")
    except mysql.connector.errors.DatabaseError as e:
        print("Транзакція 2: виникло блокування під час спроби змінити Alice", e)

    cursor1.execute("ROLLBACK")
    cursor2.execute("ROLLBACK")
    cursor1.close()
    cursor2.close()


if __name__ == "__main__":
    print("Демонстрація брудного читання (READ UNCOMMITTED):")
    dirty_read_demo()

    print("\nДемонстрація ізоляції READ COMMITTED:")
    read_committed_demo()

    print("\nДемонстрація REPEATABLE READ:")
    repeatable_read_demo()

    print("\nДемонстрація Non-repeatable Read:")
    non_repeatable_read_demo()

    print("\nДемонстрація Deadlock:")
    deadlock_demo()

# Закриття підключення після завершення роботи
connection.close()
