//! Side-by-side latency benchmark comparing current vs optimized implementations.
//!
//! Compares:
//! 1. serde_json DOM parsing vs simd-json tape parsing
//! 2. Unbounded mpsc channel vs bounded VecDeque
//!
//! Run with: cargo bench --bench ws_optimization_benchmark

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use simd_json::prelude::*;  // Required for .get() on simd-json values
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

// ============================================================================
// Test Data - Simulates real Polymarket WebSocket book message
// ============================================================================

fn generate_book_message(levels: usize) -> String {
    let mut json = String::new();
    json.push_str(r#"{"event_type":"book","asset_id":"test_asset_123","market":"0xabc123","timestamp":1706900000000,"bids":["#);
    
    for i in 0..levels {
        if i > 0 { json.push(','); }
        let price = 0.75 - (i as f64 * 0.01);
        json.push_str(&format!(r#"{{"price":"{:.4}","size":"100.0000"}}"#, price));
    }
    
    json.push_str(r#"],"asks":["#);
    
    for i in 0..levels {
        if i > 0 { json.push(','); }
        let price = 0.76 + (i as f64 * 0.01);
        json.push_str(&format!(r#"{{"price":"{:.4}","size":"100.0000"}}"#, price));
    }
    
    json.push_str("]}");
    json
}

fn generate_array_book_message(books: usize, levels_per_book: usize) -> String {
    let mut json = String::new();
    json.push('[');
    
    for b in 0..books {
        if b > 0 { json.push(','); }
        json.push_str(&generate_book_message(levels_per_book));
    }
    
    json.push(']');
    json
}

// ============================================================================
// Parsed Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderSummary {
    pub price: String,
    pub size: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookUpdate {
    pub event_type: String,
    pub asset_id: String,
    pub market: String,
    pub timestamp: u64,
    pub bids: Vec<OrderSummary>,
    pub asks: Vec<OrderSummary>,
}

#[derive(Debug, Clone)]
pub enum StreamMessage {
    Book(BookUpdate),
    Heartbeat,
}

// ============================================================================
// APPROACH A: Current Fork (serde_json DOM)
// ============================================================================

fn parse_serde_dom(text: &str) -> Result<StreamMessage, serde_json::Error> {
    let value: Value = serde_json::from_str(text)?;
    
    let event_type = value
        .get("event_type")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    
    if event_type == "book" {
        let data: BookUpdate = serde_json::from_value(value)?;
        Ok(StreamMessage::Book(data))
    } else {
        Ok(StreamMessage::Heartbeat)
    }
}

fn parse_serde_array(text: &str) -> Result<Vec<StreamMessage>, serde_json::Error> {
    let arr: Vec<Value> = serde_json::from_str(text)?;
    let mut results = Vec::with_capacity(arr.len());
    
    for value in arr {
        let event_type = value
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        
        if event_type == "book" {
            let data: BookUpdate = serde_json::from_value(value.clone())?;
            results.push(StreamMessage::Book(data));
        }
    }
    
    Ok(results)
}

// ============================================================================
// APPROACH B: Optimized (simd-json with reusable buffers)
// ============================================================================

/// Reusable processor that avoids allocations after warmup
pub struct SimdJsonProcessor {
    buffers: simd_json::Buffers,
}

impl SimdJsonProcessor {
    pub fn new(input_size_hint: usize) -> Self {
        Self {
            buffers: simd_json::Buffers::new(input_size_hint),
        }
    }
    
    /// Parse using simd-json's owned API (still builds DOM but faster)
    pub fn parse_owned(&mut self, text: &str) -> Result<StreamMessage, simd_json::Error> {
        let mut bytes = text.as_bytes().to_vec();
        let value: simd_json::OwnedValue = simd_json::to_owned_value(&mut bytes)?;
        
        let event_type = value
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        
        if event_type == "book" {
            // Extract fields directly without full deserialization
            let asset_id = value.get("asset_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let market = value.get("market").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let timestamp = value.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0);
            
            let bids = extract_levels(&value, "bids");
            let asks = extract_levels(&value, "asks");
            
            Ok(StreamMessage::Book(BookUpdate {
                event_type: "book".to_string(),
                asset_id,
                market,
                timestamp,
                bids,
                asks,
            }))
        } else {
            Ok(StreamMessage::Heartbeat)
        }
    }
    
    /// Parse mutable bytes in-place (zero-copy for the parse step)
    pub fn parse_inplace(&mut self, bytes: &mut [u8]) -> Result<StreamMessage, simd_json::Error> {
        let value: simd_json::BorrowedValue = simd_json::to_borrowed_value_with_buffers(bytes, &mut self.buffers)?;
        
        let event_type = value
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        
        if event_type == "book" {
            let asset_id = value.get("asset_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let market = value.get("market").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let timestamp = value.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0);
            
            let bids = extract_levels_borrowed(&value, "bids");
            let asks = extract_levels_borrowed(&value, "asks");
            
            Ok(StreamMessage::Book(BookUpdate {
                event_type: "book".to_string(),
                asset_id,
                market,
                timestamp,
                bids,
                asks,
            }))
        } else {
            Ok(StreamMessage::Heartbeat)
        }
    }
}

fn extract_levels(value: &simd_json::OwnedValue, key: &str) -> Vec<OrderSummary> {
    value
        .get(key)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|level| {
                    let price = level.get("price").and_then(|v| v.as_str())?.to_string();
                    let size = level.get("size").and_then(|v| v.as_str())?.to_string();
                    Some(OrderSummary { price, size })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn extract_levels_borrowed(value: &simd_json::BorrowedValue, key: &str) -> Vec<OrderSummary> {
    value
        .get(key)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|level| {
                    let price = level.get("price").and_then(|v| v.as_str())?.to_string();
                    let size = level.get("size").and_then(|v| v.as_str())?.to_string();
                    Some(OrderSummary { price, size })
                })
                .collect()
        })
        .unwrap_or_default()
}

// ============================================================================
// CHANNEL BENCHMARKS
// ============================================================================

/// Unbounded channel (current fork approach)
fn bench_unbounded_channel(messages: usize) -> Duration {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    
    rt.block_on(async {
        let (tx, mut rx) = mpsc::unbounded_channel::<StreamMessage>();
        
        let start = Instant::now();
        
        // Send all messages
        for _ in 0..messages {
            tx.send(StreamMessage::Heartbeat).unwrap();
        }
        
        // Receive all messages
        for _ in 0..messages {
            let _ = rx.recv().await.unwrap();
        }
        
        start.elapsed()
    })
}

/// Bounded VecDeque (origin approach)
fn bench_bounded_vecdeque(messages: usize, capacity: usize) -> (Duration, usize) {
    let mut queue: VecDeque<StreamMessage> = VecDeque::with_capacity(capacity);
    let mut dropped = 0usize;
    
    let start = Instant::now();
    
    // Enqueue with drop-oldest policy
    for _ in 0..messages {
        if queue.len() >= capacity {
            queue.pop_front();
            dropped += 1;
        }
        queue.push_back(StreamMessage::Heartbeat);
    }
    
    // Dequeue all
    while queue.pop_front().is_some() {}
    
    (start.elapsed(), dropped)
}

// ============================================================================
// CRITERION BENCHMARKS
// ============================================================================

fn bench_json_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_parsing");
    
    for levels in [4, 16, 64] {
        let message = generate_book_message(levels);
        let msg_size = message.len();
        
        group.throughput(Throughput::Bytes(msg_size as u64));
        
        // Serde DOM (current)
        group.bench_with_input(
            BenchmarkId::new("serde_dom", levels),
            &message,
            |b, msg| {
                b.iter(|| {
                    parse_serde_dom(black_box(msg)).unwrap()
                })
            },
        );
        
        // Simd-json owned
        let mut processor = SimdJsonProcessor::new(msg_size);
        group.bench_with_input(
            BenchmarkId::new("simd_owned", levels),
            &message,
            |b, msg| {
                b.iter(|| {
                    processor.parse_owned(black_box(msg)).unwrap()
                })
            },
        );
        
        // Simd-json in-place (true zero-copy parse)
        let mut processor2 = SimdJsonProcessor::new(msg_size);
        group.bench_with_input(
            BenchmarkId::new("simd_inplace", levels),
            &message,
            |b, msg| {
                b.iter_batched(
                    || msg.as_bytes().to_vec(),
                    |mut bytes| {
                        processor2.parse_inplace(black_box(&mut bytes)).unwrap()
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }
    
    group.finish();
}

fn bench_array_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_parsing");
    
    // Simulate initial book snapshot (multiple books in array)
    let message = generate_array_book_message(4, 16);
    let msg_size = message.len();
    
    group.throughput(Throughput::Bytes(msg_size as u64));
    
    // Serde with clone (current fork)
    group.bench_function("serde_array_clone", |b| {
        b.iter(|| {
            parse_serde_array(black_box(&message)).unwrap()
        })
    });
    
    group.finish();
}

fn bench_channels(c: &mut Criterion) {
    let mut group = c.benchmark_group("channel_throughput");
    
    for msg_count in [100, 1000, 10000] {
        // Unbounded mpsc (current)
        group.bench_with_input(
            BenchmarkId::new("unbounded_mpsc", msg_count),
            &msg_count,
            |b, &count| {
                b.iter(|| {
                    bench_unbounded_channel(count)
                })
            },
        );
        
        // Bounded VecDeque (origin)
        group.bench_with_input(
            BenchmarkId::new("bounded_vecdeque_1024", msg_count),
            &msg_count,
            |b, &count| {
                b.iter(|| {
                    bench_bounded_vecdeque(count, 1024)
                })
            },
        );
    }
    
    group.finish();
}

fn bench_latency_histogram(c: &mut Criterion) {
    let mut group = c.benchmark_group("latency_histogram");
    group.sample_size(1000);  // More samples for latency distribution
    
    let message = generate_book_message(16);
    let msg_size = message.len();
    
    // Serde latency distribution
    group.bench_function("serde_latency", |b| {
        b.iter(|| {
            let start = Instant::now();
            let _ = parse_serde_dom(black_box(&message));
            start.elapsed()
        })
    });
    
    // Simd-json latency distribution  
    let mut processor = SimdJsonProcessor::new(msg_size);
    group.bench_function("simd_latency", |b| {
        b.iter_batched(
            || message.as_bytes().to_vec(),
            |mut bytes| {
                let start = Instant::now();
                let _ = processor.parse_inplace(black_box(&mut bytes));
                start.elapsed()
            },
            criterion::BatchSize::SmallInput,
        )
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_json_parsing,
    bench_array_parsing,
    bench_channels,
    bench_latency_histogram,
);

criterion_main!(benches);
