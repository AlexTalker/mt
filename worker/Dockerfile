FROM archlinux/base
RUN pacman -Syu --noconfirm && pacman -S --noconfirm python python-redis python-pyzmq

ADD . /app

CMD ["python", "/app/worker.py"]
