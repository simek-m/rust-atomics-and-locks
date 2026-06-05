use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::{Acquire, Release};

#[derive(Default)]
pub struct Spinlock<T> {
    is_locked: AtomicBool,
    data: UnsafeCell<T>,
}

pub struct Guard<'a, T> {
    lock: &'a Spinlock<T>,
}

unsafe impl<T> Send for Guard<'_, T> where T: Send {}
unsafe impl<T> Sync for Guard<'_, T> where T: Sync {}

impl<T> Deref for Guard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // Safety: Guard can only be
        // obtained by acquiring the lock.
        unsafe { &*self.lock.data.get() }
    }
}

impl<T> DerefMut for Guard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // Safety: Guard can only be
        // obtained by acquiring the lock.
        unsafe { &mut *self.lock.data.get() }
    }
}

impl<T> Drop for Guard<'_, T> {
    fn drop(&mut self) {
        self.lock.unlock();
    }
}

unsafe impl<T> Sync for Spinlock<T> where T: Send {}

impl<'a, T> Spinlock<T> {
    pub const fn new(data: T) -> Self {
        Self {
            is_locked: AtomicBool::new(false),
            data: UnsafeCell::new(data),
        }
    }

    pub fn lock(&'a self) -> Guard<'a, T> {
        while self.is_locked.swap(true, Acquire) {
            std::hint::spin_loop();
        }

        Guard { lock: self }
    }

    fn unlock(&self) {
        self.is_locked.swap(false, Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let lock = Spinlock::new(0u64);

        std::thread::scope(|s| {
            s.spawn(|| {
                for _ in 0..1_000_000 {
                    *lock.lock() += 1;
                }
            });
            s.spawn(|| {
                for _ in 0..1_000_000 {
                    let mut g = lock.lock();
                    *g += 1;
                    drop(g);
                }
            });
        });

        assert_eq!(*lock.lock(), 2_000_000);
    }
}
