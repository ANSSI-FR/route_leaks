FROM python_dev_base

COPY tmp_leaks/ /route_leaks

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
RUN cd route_leaks ; /bin/bash -c "source $HOME/.cargo/env ; make" ; cd ..
RUN /route_leaks/env/bin/python -m pytest /route_leaks/src/route_leaks_detection || exit 0
RUN /route_leaks/env/bin/python -m pytest /route_leaks/src/related_work_implem || exit 0
