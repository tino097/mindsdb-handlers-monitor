# 🔍 MindsDB Database Handlers Monitor

> Automated testing and monitoring suite for MindsDB database integrations

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

## 📖 What is This?

This repository provides **automated testing and continuous monitoring** for [MindsDB](https://mindsdb.com/)'s database handlers. It ensures that MindsDB can reliably connect to and query various database systems including PostgreSQL, Oracle, and MS SQL Server.

### 🎯 Why Does This Exist?

- **Quality Assurance**: Catch integration issues before they reach production
- **Continuous Monitoring**: Automated tests run on every change via GitHub Actions
- **Documentation**: Each handler includes comprehensive examples and test queries
- **Regression Prevention**: Ensure new changes don't break existing functionality
- **Performance Tracking**: Monitor query execution times and identify bottlenecks

## 🗄️ Supported Databases

| Database | Status | Tests | Documentation |
|----------|--------|-------|---------------|
| **PostgreSQL** | [![PostgreSQL CI](https://github.com/tino097/mindsdb-handlers-monitor/actions/workflows/postgres.yml/badge.svg)](https://github.com/tino097/mindsdb-handlers-monitor/actions/workflows/postgres.yml) | 41 tests | [📝 Docs](./postgres/README.md) |
| **Oracle XE** | [![Oracle CI](https://github.com/tino097/mindsdb-handlers-monitor/actions/workflows/oracle.yml/badge.svg)](https://github.com/tino097/mindsdb-handlers-monitor/actions/workflows/oracle.yml) | 27 tests | [📝 Docs](./oracle/README.md) |
| **MS SQL Server** | [![MS SQL CI](https://github.com/tino097/mindsdb-handlers-monitor/actions/workflows/mssql.yml/badge.svg)](https://github.com/tino097/mindsdb-handlers-monitor/actions/workflows/mssql.yml) | 31 tests | [📝 Docs](./mssql/README.md) |
| **Elasticsearch** | [![Elasticsearch CI](https://github.com/tino097/mindsdb-handlers-monitor/actions/workflows/elasticsearch.yml/badge.svg)](https://github.com/tino097/mindsdb-handlers-monitor/actions/workflows/elasticsearch.yml) | 48 tests | [📝 Docs](./elasticsearch/README.md) |

## ✨ Features

### 🧪 Comprehensive Test Coverage

Each database handler includes tests for:

- ✅ Basic connectivity and authentication
- ✅ Table listing and schema introspection
- ✅ Simple and complex SQL queries
- ✅ JOINs across multiple tables
- ✅ Aggregations and GROUP BY operations
- ✅ Database-specific functions (date, string, numeric)
- ✅ TPC-H benchmark queries (where applicable)
- ✅ Error handling and edge cases

### 🔄 CI/CD Integration

- **Automated Testing**: Tests run on every push and pull request
- **Multiple Environments**: Tests against real database instances in Docker
- **Fast Feedback**: Results available within minutes
- **Coverage Reports**: Track test coverage for each handler

### 🐳 Local Development Support

Each handler includes Docker Compose setup for:

- Quick local testing without manual database installation
- Consistent test environments
- Easy data reset and reinitialization

## 🚀 Quick Start

### Clone and Setup

```bash
# Clone the repository
git clone https://github.com/tino097/mindsdb-handlers-monitor.git
cd mindsdb-handlers-monitor

# Choose a database handler to work with
cd postgresql  # or: oracle, mssql
```

## 📊 Test Structure

Each database handler follows a consistent structure:

```
database/
├── sql/
│   ├── init_schema.sql        # Database schema
│   └── load_data.sql          # Test data
├── tests/
│   ├── conftest.py            # Test fixtures
│   ├── test_handler.py        # Handler-specific tests
│   └── test_tpch_queries.py   # Benchmark queries (optional)
├── scripts/
│   └── setup.sh              # Setup automation
├── README.md                  # Handler documentation
└── requirements.txt          # Python dependencies
```

## 🤝 Contributing

Contributions are welcome! Here's how you can help:

### Adding a New Database Handler

1. Create a new directory: `database_name/`
2. Add test suite following the structure above
3. Create GitHub Actions workflow: `.github/workflows/database_name.yml`
4. Update this README with badge and documentation link

### Improving Existing Tests

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/improve-tests`
3. Add or modify tests
4. Ensure all tests pass: `pytest tests/ -v`
5. Submit a pull request

### Guidelines

- Write clear, descriptive test names
- Include docstrings explaining what each test validates
- Follow existing code style and patterns
- Update documentation when adding features
- Ensure CI passes before submitting PR

## 🔗 Useful Links

- **MindsDB Documentation**: <https://docs.mindsdb.com/>
- **PostgreSQL Handler**: <https://docs.mindsdb.com/integrations/data-integrations/postgresql>
- **Oracle Handler**: <https://docs.mindsdb.com/integrations/data-integrations/oracle>
- **MS SQL Handler**: <https://docs.mindsdb.com/integrations/data-integrations/microsoft-sql-server>
- **TPC-H Benchmark**: <http://www.tpc.org/tpch/>

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **MindsDB Team** - For creating an amazing ML platform
- **TPC Organization** - For the TPC-H benchmark specification
- **Database Vendors** - For providing open-source/developer editions

## 📬 Contact

- **Issues**: [GitHub Issues](https://github.com/tino097/mindsdb-handlers-monitor/issues)

---

**Made with ❤️ for the MindsDB community**

*Last updated: 2023-10-05*
