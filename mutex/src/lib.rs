use atomic_wait::{wait, wake_all, wake_one};
use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicU32, AtomicUsize};

pub struct Mutex<T> {
    state: AtomicU32,
    value: UnsafeCell<T>,
}

unsafe impl<T> Sync for Mutex<T> where T: Send {}

pub struct MutexGuard<'a, T> {
    mutex: &'a Mutex<T>,
}

unsafe impl<T> Send for MutexGuard<'_, T> where T: Send {}
unsafe impl<T> Sync for MutexGuard<'_, T> where T: Sync {}

impl<T> Deref for MutexGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { &*self.mutex.value.get() }
    }
}

impl<T> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.mutex.value.get() }
    }
}

impl<T> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        self.mutex.state.store(0, Release);
        wake_one(&self.mutex.state);
    }
}

impl<T> Mutex<T> {
    pub const fn new(value: T) -> Self {
        Self {
            state: AtomicU32::new(0),
            value: UnsafeCell::new(value),
        }
    }

    pub fn lock(&self) -> MutexGuard<'_, T> {
        while self.state.swap(1, Acquire) == 1 {
            wait(&self.state, 1);
        }
        MutexGuard { mutex: self }
    }
}

pub struct Condvar {
    counter: AtomicU32,
    num_waiters: AtomicUsize,
}

impl Default for Condvar {
    fn default() -> Self {
        Self::new()
    }
}

impl Condvar {
    pub const fn new() -> Self {
        Self {
            counter: AtomicU32::new(0),
            num_waiters: AtomicUsize::new(0),
        }
    }

    pub fn notify_one(&self) {
        if self.num_waiters.load(Relaxed) > 0 {
            self.counter.fetch_add(1, Relaxed);
            wake_one(&self.counter);
        }
    }

    pub fn notify_all(&self) {
        if self.num_waiters.load(Relaxed) > 0 {
            self.counter.fetch_add(1, Relaxed);
            wake_all(&self.counter);
        }
    }

    pub fn wait<'a, T>(&self, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
        self.num_waiters.fetch_add(1, Relaxed); // New!

        let counter_value = self.counter.load(Relaxed);

        let mutex = guard.mutex;
        drop(guard);

        wait(&self.counter, counter_value);

        self.num_waiters.fetch_sub(1, Relaxed); // New!

        mutex.lock()
    }
}

pub struct RwLock<T> {
    state: AtomicU32,
    value: UnsafeCell<T>,
    writer_wake_counter: AtomicU32,
}

unsafe impl<T> Sync for RwLock<T> where T: Send {}

impl<T> RwLock<T> {
    pub const fn new(value: T) -> Self {
        Self {
            state: AtomicU32::new(0),
            writer_wake_counter: AtomicU32::new(0), // New!
            value: UnsafeCell::new(value),
        }
    }

    pub fn read(&self) -> ReadGuard<'_, T> {
        let mut s = self.state.load(Relaxed);
        loop {
            if s.is_multiple_of(2) {
                // Even.
                assert!(s < u32::MAX - 2, "too many readers");
                match self.state.compare_exchange_weak(s, s + 2, Acquire, Relaxed) {
                    Ok(_) => return ReadGuard { rwlock: self },
                    Err(e) => s = e,
                }
            }
            if !s.is_multiple_of(2) {
                wait(&self.state, s);
                s = self.state.load(Relaxed);
            }
        }
    }

    pub fn write(&self) -> WriteGuard<'_, T> {
        let mut s = self.state.load(Relaxed);
        loop {
            // Try to lock if unlocked.
            if s <= 1 {
                match self.state.compare_exchange(s, u32::MAX, Acquire, Relaxed) {
                    Ok(_) => return WriteGuard { rwlock: self },
                    Err(e) => {
                        s = e;
                        continue;
                    }
                }
            }
            // Block new readers, by making sure the state is odd.
            if s.is_multiple_of(2) {
                match self.state.compare_exchange(s, s + 1, Relaxed, Relaxed) {
                    Ok(_) => {}
                    Err(e) => {
                        s = e;
                        continue;
                    }
                }
            }
            // Wait, if it's still locked.
            let w = self.writer_wake_counter.load(Acquire);
            s = self.state.load(Relaxed);
            if s >= 2 {
                wait(&self.writer_wake_counter, w);
                s = self.state.load(Relaxed);
            }
        }
    }
}

pub struct ReadGuard<'a, T> {
    rwlock: &'a RwLock<T>,
}

impl<T> Drop for ReadGuard<'_, T> {
    fn drop(&mut self) {
        // Decrement the state by 2 to remove one read-lock.
        if self.rwlock.state.fetch_sub(2, Release) == 3 {
            // If we decremented from 3 to 1, that means
            // the RwLock is now unlocked _and_ there is
            // a waiting writer, which we wake up.
            self.rwlock.writer_wake_counter.fetch_add(1, Release);
            wake_one(&self.rwlock.writer_wake_counter);
        }
    }
}

pub struct WriteGuard<'a, T> {
    rwlock: &'a RwLock<T>,
}

impl<T> Deref for WriteGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { &*self.rwlock.value.get() }
    }
}

impl<T> Drop for WriteGuard<'_, T> {
    fn drop(&mut self) {
        self.rwlock.state.store(0, Release);
        self.rwlock.writer_wake_counter.fetch_add(1, Release);
        wake_one(&self.rwlock.writer_wake_counter);
        wake_all(&self.rwlock.state);
    }
}

impl<T> DerefMut for WriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.rwlock.value.get() }
    }
}

impl<T> Deref for ReadGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { &*self.rwlock.value.get() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_condvar() {
        let mutex = Mutex::new(0);
        let condvar = Condvar::new();

        let mut wakeups = 0;

        thread::scope(|s| {
            s.spawn(|| {
                thread::sleep(Duration::from_secs(1));
                *mutex.lock() = 123;
                condvar.notify_one();
            });

            let mut m = mutex.lock();
            while *m < 100 {
                m = condvar.wait(m);
                wakeups += 1;
            }

            assert_eq!(*m, 123);
        });

        // Check that the main thread actually did wait (not busy-loop),
        // while still allowing for a few spurious wake ups.
        assert!(wakeups < 10);
    }

    #[test]
    fn test_mutex() {
        let m = Mutex::new(0);
        thread::scope(|s| {
            s.spawn(|| {
                for _ in 0..100 {
                    *m.lock() += 1;
                }
            });
            s.spawn(|| {
                for _ in 0..100 {
                    *m.lock() += 1;
                }
            });
        });
        assert_eq!(*m.lock(), 200);
    }

    #[test]
    fn test_rwlock() {
        let rw = RwLock::new(0);
        thread::scope(|s| {
            s.spawn(|| {
                for _ in 0..100 {
                    let r = rw.read();
                    let _ = *r;
                }
            });
            s.spawn(|| {
                for _ in 0..100 {
                    let r = rw.read();
                    let _ = *r;
                }
            });
            s.spawn(|| {
                for _ in 0..100 {
                    *rw.write() += 1;
                }
            });
        });
        assert_eq!(*rw.read(), 100);
    }
}
