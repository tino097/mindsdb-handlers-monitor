# MindsDB PostgreSQL Handler Monitor

Automated testing for the MindsDB PostgreSQL database handler.

<!-- TEST_RESULTS_START -->
## 📊 Latest Test Results

*Test results will be automatically updated here after each workflow run.*

<!-- TEST_RESULTS_END -->

## Overview

This test suite validates the MindsDB PostgreSQL handler functionality including:

- ✅ Database connection and authentication
- 📊 Query execution (SELECT, JOIN, aggregates)
- 🔤 Data type handling (numeric, string, date/time)
- 🚨 Error handling and edge cases
- 🔄 Transaction support

## Database Configuration

| Parameter | Value |
|-----------|-------|
| **Database** | PostgreSQL 15 |
| **Test Database** | test_db |
| **Test Schema** | test_schema |
| **Port** | 5432 |

## CI/CD

Tests run automatically on:

- Every push to `main` branch (when postgres files change)
- Every pull request (when postgres files change)
- Manual workflow dispatch

### Workflow Features

- ✅ Automated PostgreSQL setup
- 📊 Test data loading and verification
- 🚀 MindsDB installation and startup
- 🧪 Comprehensive test execution
- 📈 Test reports with coverage
- 📦 Artifact storage (logs, reports)
- 🔄 Automatic README updates

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines on:

- Adding new test cases
- Modifying test data
- Updating documentation

## Resources

- [MindsDB Documentation](https://docs.mindsdb.com)
- [PostgreSQL Handler Docs](https://docs.mindsdb.com/integrations/data-integrations/postgresql)
- [GitHub Actions Results](../../actions/workflows/postgres-ci.yml)

---

**Note:** Test results section above is automatically updated by CI/CD. Do not manually edit the area between `<!-- TEST_RESULTS_START -->` and `<!-- TEST_RESULTS_END -->` markers.
