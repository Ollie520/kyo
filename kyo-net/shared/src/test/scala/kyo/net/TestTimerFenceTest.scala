package kyo.net

import kyo.*

class TestTimerFenceTest extends Test:

    import AllowUnsafe.embrace.danger

    "armedThenAdvance fails naming the timer when none is armed" in {
        Clock.withTimeControl { tc =>
            Clock.get.map { clock =>
                val other = clock.unsafe.sleep(2.seconds)
                awaitArmed(tc, "close-flush grace", 1.second, 50.millis).map { missing =>
                    assert(!other.done(), "a sleeper of another duration must not be advanced")
                    assert(missing.exists(_.startsWith(s"the close-flush grace (${1.second.show}) was never armed")), s"got $missing")
                }
            }
        }
    }

    "armedThenAdvance fires the timer once it is armed" in {
        Clock.withTimeControl { tc =>
            Clock.get.map { clock =>
                val timer = clock.unsafe.sleep(1.second)
                armedThenAdvance(tc, "close-flush grace", 1.second).andThen(assert(timer.done(), "the armed timer must have fired"))
            }
        }
    }

end TestTimerFenceTest
