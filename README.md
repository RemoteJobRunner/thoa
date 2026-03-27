# thoa
Thoa CLI for submitting jobs to Thoa platform

## Install local virtual environment
### 1) Install dependencies
```bash
cd ~/thoa/thoa
python3 -m venv venv
source venv/bin/activate
pip install poetry==2.3.2
pip install
```
### 2) Create API key
Go to `http://localhost:${FRONTEND_PORT}/workbench/api_keys`

and add it into `.bashrc` for
- `THOA_API_KEY`:
```bash
echo 'export THOA_API_KEY="generated_api_key_here"' >> ~/.bashrc
````
- `THOA_API_URL`:
```bash
echo 'export THOA_API_URL="http://localhost:${FRONTEND_PORT}"' >> ~/.bashrc
````
### 3) Refresh console:
```bash
source ~/.bashrc
```

## Re-use CLI with a local virtual environment
### Activate venv and use it
```bash
cd ~/thoa/thoa
python3 -m venv venv
source venv/bin/activate
```

## Running tests
### 1) Install test dependencies

```bash
cd ~/thoa
source thoa/venv/bin/activate
pip install pytest
```

### 2) Run tests

```bash
pytest tests/ -v
```