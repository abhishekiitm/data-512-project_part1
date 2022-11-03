import numpy as np
import pandas as pd

from scipy.integrate import solve_ivp
from datetime import datetime, timedelta


def sirs_equation(t, y, gamma, beta, delta, N):
    st = y[0]
    it = y[1]
    rt = y[2]

    dst_dt = -beta*st*it/N + delta*rt
    dit_dt = beta*st*it/N - gamma*it
    drt_dt = gamma*it - delta*rt

    return np.array([dst_dt, dit_dt, drt_dt])


class SIRSModel(object):
    def __init__(self, s0: float, i0: float, r0: float, first_infection: datetime, 
            end_date: datetime, N: float, beta: float, 
            gamma: float, delta: float) -> None:
        self.s0 = s0
        self.i0 = i0
        self.r0 = r0
        self.first_infection = first_infection
        self.end_date = end_date
        self.N = N
        self.beta = beta
        self.gamma = gamma
        self.delta = delta
        self.s_t = [s0]
        self.i_t = [i0]
        self.r_t = [r0]
        self.date = [first_infection]

    def simulate(self):
        idx = 0
        while self.date[-1] < self.end_date:
            t_span = [0, 1]
            y0 = [self.s_t[-1], self.i_t[-1], self.r_t[-1]]
            res = solve_ivp(sirs_equation, t_span=t_span, y0=y0, args=(self.gamma, self.beta, self.delta, self.N))

            s_t = res['y'][0][-1]
            i_t = res['y'][1][-1]
            r_t = res['y'][2][-1]
            next_date = self.date[-1] + timedelta(days=1)

            self.s_t.append(s_t)
            self.i_t.append(i_t)
            self.r_t.append(r_t)
            self.date.append(next_date)

            idx+=1

        return self.s_t, self.i_t, self.r_t, self.date



