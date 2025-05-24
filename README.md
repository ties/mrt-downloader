# Download MRT updates

Download MRT updates. Just from RIPE RIS for now.

```
# install the tool using pipx
pipx install git+https://github.com/ties/mrt-downloader.git
# Now the command should be available as `mrt-downloader`

# let's assume the 'mrt' directory exists
mkdir mrt

# And download a day's worth of mrt files to this directory
mrt-downloader mrt 2025-01-16T00:50 2025-01-17T00:00
```

# Installation

This project requires python 3.11 or higher. Some operating systems may not
include this python version. In turn, this means that you may need to use
install a higher python version first. An example of this on Rocky Linux 9 is
given below.

There are two ways to use this project:

  * Install the command-line tool using pipx
  * As a checked out python project, using uv
  * Or as a library (there is no documentation for this at the moment).

### pipx

```
pipx install git+https://github.com/ties/mrt-downloader.git
# Now the command should be available as `mrt-downloader`
mrt-downloader mrt 2025-01-16T00:50 2025-01-17T00:00
```
### uv

**Only recommended when editing the project**
```
# install dependencies
uv install
# download a day's MRT files into the mrt directory.
uv run python -m mrt_downloader.cli mrt 2025-01-16T00:50 2025-01-17T00:00
```

## Full example: Running on Rocky Linux 9

These steps were done on a clean Rocky Linux 9 VM (Red Hat 9 equivalent):
  * Install `git` and `python3.12` and `pip` for python 3.12
  * Install `pipx`
  * Install and run `mrt-downloader`.

```
[root@rocky-32gb-fsn1-1 ~]# dnf install -y git python3.12 python3.12-pip
...
  python3.12-pip-23.2.1-4.el9.noarch                   python3.12-pip-wheel-23.2.1-4.el9.noarch
  python3.12-setuptools-68.2.2-4.el9.noarch

Complete!
[root@rocky-32gb-fsn1-1 ~]# pip3.12 install pipx
...
Installing collected packages: platformdirs, packaging, click, argcomplete, userpath, pipx
Successfully installed argcomplete-3.5.3 click-8.1.8 packaging-24.2 pipx-1.7.1 platformdirs-4.3.6 userpath-1.9.2
WARNING: Running pip as the 'root' user can result in broken permissions and conflicting behaviour with the system package manager. It is recommended to use a virtual environment instead: https://pip.pypa.io/warnings/venv
[root@rocky-32gb-fsn1-1 ~]# pipx install git+https://github.com/ties/mrt-downloader.git
  installed package mrt-downloader 0.0.1, installed using Python 3.12.5
  These apps are now globally available
    - mrt-downloader
done! ✨ 🌟 ✨
[root@rocky-32gb-fsn1-1 ~]# mkdir /tmp/mrt
[root@rocky-32gb-fsn1-1 ~]# mrt-downloader /tmp/mrt 2025-01-16T00:50 2025-01-17T00:00
Downloading updates from 2025-01-16 00:50:00 to 2025-01-17 00:00:00 to /tmp/mrt
Skipping https://data.ris.ripe.net/rrc27/2025.01/ due to HTTP error 404
Skipping https://data.ris.ripe.net/rrc08/2025.01/ due to HTTP error 404
Skipping https://data.ris.ripe.net/rrc09/2025.01/ due to HTTP error 404
Skipping https://data.ris.ripe.net/rrc02/2025.01/ due to HTTP error 404
Skipping https://data.ris.ripe.net/rrc17/2025.01/ due to HTTP error 404
Downloading 6486 files on 16 workers

Downloaded 316 files
Downloaded 349 files
Downloaded 314 files
Downloaded 345 files
Downloaded 422 files
Downloaded 562 files
Downloaded 475 files
Downloaded 392 files
Downloaded 560 files
Downloaded 443 files
Downloaded 408 files
Downloaded 455 files
Downloaded 256 files
Downloaded 269 files
Downloaded 521 files
Downloaded 399 files
[root@rocky-32gb-fsn1-1 ~]#
[root@rocky-32gb-fsn1-1 ~]# ls /tmp/mrt/
rrc00-bview.20250116.0800.gz    rrc10-updates.20250116.1615.gz  rrc19-updates.20250116.0825.gz
rrc00-bview.20250116.1600.gz    rrc10-updates.20250116.1620.gz  rrc19-updates.20250116.0830.gz
rrc00-bview.20250117.0000.gz    rrc10-updates.20250116.1625.gz  rrc19-updates.20250116.0835.gz
rrc00-updates.20250116.0050.gz  rrc10-updates.20250116.1630.gz  rrc19-updates.20250116.0840.gz
...
rrc10-updates.20250116.1555.gz  rrc19-updates.20250116.0805.gz  rrc26-updates.20250116.2345.gz
rrc10-updates.20250116.1600.gz  rrc19-updates.20250116.0810.gz  rrc26-updates.20250116.2350.gz
rrc10-updates.20250116.1605.gz  rrc19-updates.20250116.0815.gz  rrc26-updates.20250116.2355.gz
rrc10-updates.20250116.1610.gz  rrc19-updates.20250116.0820.gz  rrc26-updates.20250117.0000.gz
[root@rocky-32gb-fsn1-1 ~]#
```

