[![website](https://img.shields.io/website?url=https://www.aneur.info&style=flat)](https://www.aneur.info/recce)
[![release](https://img.shields.io/github/v/release/aneurinsmith/Recce?include_prereleases&style=flat)](https://github.com/aneurinsmith/Recce/releases/latest)

### Create SymLinks

```bash
sudo ln -s recce.conf /etc/nginx/conf.d/
sudo ln -s recce.service /etc/systemd/system/
sudo systemctl start recce
```

### Load Neo4j data

```bash
sudo python3 etl.py
```
