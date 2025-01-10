## v0.3.0

### 🐛 Bug fixes

* Fix default `unique_extra_keys` value [[2039c89](https://github.com/aiidateam/aiida-submission-controller/commit/2039c89ff07261d6799537a6a2fc12d586a6abc8)]

### 📚 Documentation

* Load profile in `add_in_batches` example [[984dae6](https://github.com/aiidateam/aiida-submission-controller/commit/984dae636ec991cc9852c119b3ca0da45cf970d3)]

### 🔧 Maintenance

* Update `pre-commit` to `~=3.8` [[b12d0f4](https://github.com/aiidateam/aiida-submission-controller/commit/b12d0f4bf9b15728ee44fb676c74663f952307d2)]
* Update Python versions [[036f98f](https://github.com/aiidateam/aiida-submission-controller/commit/036f98f9e546bb62d2f5e562e5b03f7a623830e8)]

### ⬆️ Update dependencies

* Upgrade to `aiida-core~=2.5` and `pydantic~=2.8` [[b861256](https://github.com/aiidateam/aiida-submission-controller/commit/b861256c0a09d6fa043b2d6aa6439e757e5a9e02)]

## v0.2.0

### ‼️ Breaking changes

* Prioritize the `order_by` approach to sorting submissions [[201cdac](https://github.com/aiidateam/aiida-submission-controller/commit/201cdac39d32c8a9a70845c929ba6e82664471f5)]
* Redesign controllers as `pydantic` `BaseModel`s [[a3b1d33](https://github.com/aiidateam/aiida-submission-controller/commit/a3b1d33f3304940e23ff0eef394e6d5d1c433882)]

### ✨ New features

* Add optional delay between submissions [[ffafbf5](https://github.com/aiidateam/aiida-submission-controller/commit/ffafbf5bef6524ad427a2bb023dbc43119963c37)]
* Add the `order_by` field to the `FromGroupSubmissionController` [[ab943f6](https://github.com/aiidateam/aiida-submission-controller/commit/ab943f670b90f1937e5df34d976aecba8f238842)]
* Print a table with the status when submitting [[a53f0c5](https://github.com/aiidateam/aiida-submission-controller/commit/a53f0c5364a40e38a02f5bfc0079bcc5b499dbe5)]
* `FromGroupSubmissionController`: Add `filters` [[76eb0b0](https://github.com/aiidateam/aiida-submission-controller/commit/76eb0b006d00ac9a262467dce7016095593bfe03)]

### 👌 Improvements

* Catch all `Exception`s during submission loop [[8dcea88](https://github.com/aiidateam/aiida-submission-controller/commit/8dcea88e11f76c62ddd315c58f499ea0a0e61051)]
* Always submit number of requested processes [[b910171](https://github.com/aiidateam/aiida-submission-controller/commit/b910171c70be9c7368f452658621f07f66e2cfad)]
* Update `.gitignore` [[1ee409a](https://github.com/aiidateam/aiida-submission-controller/commit/1ee409a2ab05a17213c7ddf4e8ea28af6126343b)]
* Make `unique_extra_keys` a `pydantic` field [[6662ac8](https://github.com/aiidateam/aiida-submission-controller/commit/6662ac86be8a99051f864ecf930d79ce88f879f5)]
* Catch and log submissions [[6b915bd](https://github.com/aiidateam/aiida-submission-controller/commit/6b915bdc3452cc8ebb36974b883e7891d94d3541)]

### 🐛 Bug fixes

* Fix number of submitted `WorkChain`s [[ce9b6f6](https://github.com/aiidateam/aiida-submission-controller/commit/ce9b6f6917b66d31b0593e287758996ef896342f)]
* Fix `submit_new_batch` when `verbose = True` [[d944b42](https://github.com/aiidateam/aiida-submission-controller/commit/d944b42695288dfa7e86d2830c725a54a3383b52)]
* Make `unique_extra_keys` field properly optional [[d811858](https://github.com/aiidateam/aiida-submission-controller/commit/d811858a1417a00f0bac45a35061d672722983db)]
* Fix `dry_run` in `BaseSubmissionController` [[f83a7f9](https://github.com/aiidateam/aiida-submission-controller/commit/f83a7f9eebd48ec513af7ba8b4c19ad15d9e4894)]
* Allow specifying nested extras [[fcfd545](https://github.com/aiidateam/aiida-submission-controller/commit/fcfd545c56ba4fea6d2607ff6b3e053180934017)]

### 📚 Documentation

* Update `add_in_batches` example [[f3f6dac](https://github.com/aiidateam/aiida-submission-controller/commit/f3f6dac9803561ff3509561fc980d1b79d0f4e09)]
* Update `PwBaseWorkChain` example [[da769d0](https://github.com/aiidateam/aiida-submission-controller/commit/da769d047bcd64aeb447e30fd5c410848f9fbc68)]

### 🔧 Maintenance

* Remove language version for black pre-commit hook [[f80d29b](https://github.com/aiidateam/aiida-submission-controller/commit/f80d29bbe058683dc784fd56918fa5f64c665b3d)]
* Add `update_changelog.py` script [[cf9d6f0](https://github.com/aiidateam/aiida-submission-controller/commit/cf9d6f0eb02091081758db215c5282c22445edc9)]
* Add CD GitHub Action [[7696919](https://github.com/aiidateam/aiida-submission-controller/commit/76969192f9fd6a392901d638e6436f825865c9bc)]
* Switch from `pylint` to `ruff` [[98771d9](https://github.com/aiidateam/aiida-submission-controller/commit/98771d95c67bf4e39d3627ddc8053647add6fd3a)]
* Switch to using `black` [[7d31f2e](https://github.com/aiidateam/aiida-submission-controller/commit/7d31f2ead9b7bf57e9f93dd981d2fd9ef99d9b88)]
* Add `isort` to `pre-commit` hooks [[b35338b](https://github.com/aiidateam/aiida-submission-controller/commit/b35338b42d5174f358980c6fb72dbea2e53d03d4)]
* Remove `.pylintrc` and fix linting [[8eef381](https://github.com/aiidateam/aiida-submission-controller/commit/8eef38136f068a0b385d5e162b85333876a425ed)]
* Switch to `pyproject.toml` [[fad21ad](https://github.com/aiidateam/aiida-submission-controller/commit/fad21ad05440966f5b770bf51b3cf8c441870187)]
