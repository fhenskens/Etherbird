//! Pluggable operation storage and scheduling policies.
use std::collections::{BinaryHeap, VecDeque};
/// Rejection from a custom queue, including bounded queues at capacity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueueError {
    Full,
    Rejected(String),
}
impl std::fmt::Display for QueueError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => formatter.write_str("operation queue is full"),
            Self::Rejected(message) => formatter.write_str(message),
        }
    }
}
impl std::error::Error for QueueError {}

/// Synchronous queue used under the pool's internal lock.
///
/// Implementations must preserve ownership of every accepted item, report the exact length,
/// and return an item from `pop` whenever nonempty. Methods must be short,
/// nonblocking, and must not reenter the pool. `retain` must visit every item and
/// drop those for which the predicate returns false. Dispatch selects from the
/// queue only after capacity is available, so policies can reconsider ordering.
/// Rejected items are dropped and their caller receives `Error::Queue`.
///
/// ```
/// use etherbird::{OperationQueue, QueueError};
/// struct Stack<T>(Vec<T>);
/// impl<T: Send> OperationQueue<T> for Stack<T> {
///     fn push(&mut self, item: T) -> Result<(), QueueError> { self.0.push(item); Ok(()) }
///     fn pop(&mut self) -> Option<T> { self.0.pop() }
///     fn len(&self) -> usize { self.0.len() }
///     fn retain(&mut self, predicate: &mut dyn FnMut(&T) -> bool) { self.0.retain(predicate); }
/// }
/// let mut queue = Stack(Vec::new());
/// queue.push(1).unwrap();
/// queue.push(2).unwrap();
/// assert_eq!(queue.pop(), Some(2));
/// ```
pub trait OperationQueue<T>: Send {
    fn push(&mut self, item: T) -> Result<(), QueueError>;
    fn pop(&mut self) -> Option<T>;
    fn len(&self) -> usize;
    fn retain(&mut self, predicate: &mut dyn FnMut(&T) -> bool);
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn clear(&mut self) {
        while self.pop().is_some() {}
    }
}

/// First-in, first-out operation queue. Priority metadata does not affect ordering.
pub struct FifoQueue<T> {
    items: VecDeque<T>,
}
impl<T> Default for FifoQueue<T> {
    fn default() -> Self {
        Self {
            items: VecDeque::new(),
        }
    }
}
impl<T: Send> OperationQueue<T> for FifoQueue<T> {
    fn push(&mut self, item: T) -> Result<(), QueueError> {
        self.items.push_back(item);
        Ok(())
    }
    fn pop(&mut self) -> Option<T> {
        self.items.pop_front()
    }
    fn len(&self) -> usize {
        self.items.len()
    }
    fn retain(&mut self, predicate: &mut dyn FnMut(&T) -> bool) {
        self.items.retain(predicate);
    }
    fn clear(&mut self) {
        self.items.clear();
    }
}

/// Heap queue. Queued operations sort lower priorities first and FIFO on ties.
pub struct PriorityQueue<T> {
    items: BinaryHeap<T>,
}
impl<T: Ord> Default for PriorityQueue<T> {
    fn default() -> Self {
        Self {
            items: BinaryHeap::new(),
        }
    }
}
impl<T: Ord + Send> OperationQueue<T> for PriorityQueue<T> {
    fn push(&mut self, item: T) -> Result<(), QueueError> {
        self.items.push(item);
        Ok(())
    }
    fn pop(&mut self) -> Option<T> {
        self.items.pop()
    }
    fn len(&self) -> usize {
        self.items.len()
    }
    fn retain(&mut self, predicate: &mut dyn FnMut(&T) -> bool) {
        self.items.retain(predicate);
    }
    fn clear(&mut self) {
        self.items.clear();
    }
}

/// FIFO queue with a fixed admission limit. Rejected items are dropped.
///
/// ```
/// use etherbird::{BoundedFifoQueue, OperationQueue, QueueError};
/// let mut queue = BoundedFifoQueue::new(1);
/// queue.push("first").unwrap();
/// assert_eq!(queue.push("excess"), Err(QueueError::Full));
/// assert_eq!(queue.pop(), Some("first"));
/// queue.push("next").unwrap();
/// ```
///
/// Capacity bounds waiting items, not active operations or pool resources.
/// Zero capacity rejects every push (including when a resource is ready).
/// Use with `Pool::try_new_with_queue_factory`; admission always visits this queue.
pub struct BoundedFifoQueue<T> {
    items: VecDeque<T>,
    capacity: usize,
}
impl<T> BoundedFifoQueue<T> {
    /// Construct an empty queue without preallocating its maximum capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            items: VecDeque::new(),
            capacity,
        }
    }
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}
impl<T: Send> OperationQueue<T> for BoundedFifoQueue<T> {
    fn push(&mut self, item: T) -> Result<(), QueueError> {
        if self.items.len() >= self.capacity {
            return Err(QueueError::Full);
        }
        self.items.push_back(item);
        Ok(())
    }
    fn pop(&mut self) -> Option<T> {
        self.items.pop_front()
    }
    fn len(&self) -> usize {
        self.items.len()
    }
    fn retain(&mut self, predicate: &mut dyn FnMut(&T) -> bool) {
        self.items.retain(predicate);
    }
    fn clear(&mut self) {
        self.items.clear();
    }
}
