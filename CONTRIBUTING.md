# Contributing

When contributing to the MindsDB Handlers Monitor, please first discuss the change you wish to make via issue,
email, or any other method with the maintainers of this repository before making a change.

Please note we have a code of conduct, please follow it in all your interactions with the project.

## Pull Request Process

1. Ensure any test dependencies are properly documented in `requirements.txt` for your handler.
2. Update the handler-specific README.md with details of changes to test data, schema, or CI/CD workflows.
3. Verify all tests pass locally before submitting the Pull Request.
4. Follow the data and testing guidelines outlined in the sections below.
5. You may merge the Pull Request once you have the sign-off of two other developers, or if you
   do not have permission to do that, you may request the second reviewer to merge it for you.

## Test Data Guidelines

### Data Quality Standards

- **Keep test data deterministic**: Use consistent, reproducible datasets that produce the same results across multiple test runs.
- **Use standard formats**: Store data files in `{database_name}/data/tables/` using formats like CSV, TSV, or pipe-delimited (.tbl) files.
- **Include diverse test cases**: Ensure data includes edge cases such as NULL values, special characters, and boundary conditions.
- **Avoid sensitive information**: Never include personally identifiable information (PII) or confidential data in test fixtures.
- **Document data sources**: If using standard benchmarks (e.g., TPC-H), cite the source and version.

## Testing Standards

### Test Requirements

- **Use pytest framework**: All tests should be written using pytest conventions.
- **Test isolation**: Each test should be independent and not rely on the execution order.
- **Environment variables**: Use environment variables for configuration (database credentials, API URLs).
- **Fixtures**: Define common setup/teardown logic in `conftest.py`.
- **Assertions**: Include descriptive assertion messages to aid debugging.

### Test Coverage

Your test suite should include:

- **Connection tests**: Verify the handler can connect to the database.
- **Query tests**: Validate basic SELECT, JOIN, and aggregate queries.
- **Data type tests**: Ensure all relevant data types are handled correctly.
- **Error handling tests**: Confirm proper behavior with invalid inputs or connection failures.
- **Edge case tests**: Test NULL values, empty results, and large datasets.

## CI/CD Workflow Standards

### Workflow Requirements

Each handler monitor must include a GitHub Actions workflow (`.github/workflows/ci.yml`) that:

1. Sets up the database service with proper health checks
2. Installs Python dependencies and MindsDB
3. Initializes the database schema
4. Loads test data and verifies row counts
5. Starts MindsDB and confirms it's ready
6. Runs the test suite with result tracking
7. Generates a comprehensive test summary
8. Uploads artifacts (test results, logs) for debugging

## Documentation Standards

When adding a new handler monitor:

1. Include a README.md in the handler directory explaining:
   - Database version and configuration
   - Test data description and schema
   - Special setup requirements
   - Known limitations or issues
2. Document any non-standard dependencies or configurations
3. Provide examples of expected test output
4. Update the root repository README.md to list the new handler

## Code of Conduct

### Our Pledge

In the interest of fostering an open and welcoming environment, we as
contributors and maintainers pledge to making participation in our project and
our community a harassment-free experience for everyone, regardless of age, body
size, disability, ethnicity, gender identity and expression, level of experience,
nationality, personal appearance, race, religion, or sexual identity and
orientation.

### Our Standards

Examples of behavior that contributes to creating a positive environment
include:

- Using welcoming and inclusive language
- Being respectful of differing viewpoints and experiences
- Gracefully accepting constructive criticism
- Focusing on what is best for the community
- Showing empathy towards other community members

Examples of unacceptable behavior by participants include:

- The use of sexualized language or imagery and unwelcome sexual attention or
advances
- Trolling, insulting/derogatory comments, and personal or political attacks
- Public or private harassment
- Publishing others' private information, such as a physical or electronic
  address, without explicit permission
- Other conduct which could reasonably be considered inappropriate in a
  professional setting

### Our Responsibilities

Project maintainers are responsible for clarifying the standards of acceptable
behavior and are expected to take appropriate and fair corrective action in
response to any instances of unacceptable behavior.

Project maintainers have the right and responsibility to remove, edit, or
reject comments, commits, code, wiki edits, issues, and other contributions
that are not aligned to this Code of Conduct, or to ban temporarily or
permanently any contributor for other behaviors that they deem inappropriate,
threatening, offensive, or harmful.

### Scope

This Code of Conduct applies both within project spaces and in public spaces
when an individual is representing the project or its community. Examples of
representing a project or community include using an official project e-mail
address, posting via an official social media account, or acting as an appointed
representative at an online or offline event. Representation of a project may be
further defined and clarified by project maintainers.

### Enforcement

Instances of abusive, harassing, or otherwise unacceptable behavior may be
reported by contacting the project team. All complaints will be reviewed and
investigated and will result in a response that is deemed necessary and
appropriate to the circumstances. The project team is obligated to maintain
confidentiality with regard to the reporter of an incident. Further details of
specific enforcement policies may be posted separately.

Project maintainers who do not follow or enforce the Code of Conduct in good
faith may face temporary or permanent repercussions as determined by other
members of the project's leadership.

### Attribution

This Code of Conduct is adapted from the [Contributor Covenant][homepage], version 1.4,
available at [http://contributor-covenant.org/version/1/4][version]

[homepage]: http://contributor-covenant.org
[version]: http://contributor-covenant.org/version/1/4/
