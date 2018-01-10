FROM alx/test
RUN pacman -Syu --noconfirm && pacman -S --noconfirm python python-flask python-redis python-pyzmq

ADD . /app

EXPOSE 80

CMD ["python", "/app/master.py"]
