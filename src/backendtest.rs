#[macro_export]
macro_rules! test_backend {
    ($f:expr) => {
        use crate::{AtomicWriteOperation, Backend, BatchOperation};
        use std::ops::Bound;

        #[tokio::test]
        #[serial]
        async fn test_set() {
            let b = $f().await;

            b.set("foo", "bar").await.unwrap();
            assert_eq!(b.get("foo").await.unwrap(), Some("bar".into()));
        }

        #[tokio::test]
        #[serial]
        async fn test_delete() {
            let b = $f().await;

            assert_eq!(b.delete("foo").await.unwrap(), false);

            b.set("foo", "bar").await.unwrap();
            assert_eq!(b.get("foo").await.unwrap(), Some("bar".into()));

            assert_eq!(b.delete("foo").await.unwrap(), true);
            assert_eq!(b.get("foo").await.unwrap(), None);
        }

        #[tokio::test]
        #[serial]
        async fn test_set_nx() {
            let b = $f().await;

            assert_eq!(b.set_nx("foo", "bar").await.unwrap(), true);
            assert_eq!(b.get("foo").await.unwrap(), Some("bar".into()));

            assert_eq!(b.set_nx("foo", "bar").await.unwrap(), false);
        }

        #[tokio::test]
        #[serial]
        async fn test_set_eq() {
            let b = $f().await;

            b.set("foo", "bar").await.unwrap();
            assert_eq!(b.set_eq("foo", "baz", "bar").await.unwrap(), true);
            assert_eq!(b.get("foo").await.unwrap(), Some("baz".into()));

            assert_eq!(b.set_eq("foo", "qux", "bar").await.unwrap(), false);
            assert_eq!(b.get("foo").await.unwrap(), Some("baz".into()));
        }

        #[tokio::test]
        #[serial]
        async fn test_s_add() {
            let b = $f().await;

            b.s_add("foo", "bar").await.unwrap();
            assert_eq!(vec!["bar"], b.s_members("foo").await.unwrap());

            b.s_add("foo", "baz").await.unwrap();
            b.s_add("foo", "baz").await.unwrap();
            let mut members = b.s_members("foo").await.unwrap();
            members.sort();
            assert_eq!(vec!["bar", "baz"], members);
        }

        #[tokio::test]
        #[serial]
        async fn test_h_get() {
            let b = $f().await;

            let v = b.h_get("foo", "bar").await.unwrap();
            assert_eq!(v, None);

            b.h_set("foo", [("bar", "baz")].iter().cloned()).await.unwrap();

            let v = b.h_get("foo", "bar").await.unwrap();
            assert_eq!(Some("baz".into()), v);
        }

        #[tokio::test]
        #[serial]
        async fn test_h_del() {
            let b = $f().await;

            b.h_del("foo", ["bar"].iter().cloned()).await.unwrap();

            b.h_set("foo", [("bar", "baz")].iter().cloned()).await.unwrap();

            let v = b.h_get("foo", "bar").await.unwrap();
            assert_eq!(Some("baz".into()), v);

            b.h_del("foo", ["bar"].iter().cloned()).await.unwrap();

            let v = b.h_get("foo", "bar").await.unwrap();
            assert_eq!(v, None);
        }

        #[tokio::test]
        #[serial]
        async fn test_get_all() {
            let b = $f().await;

            b.h_set("foo", [("bar", "baz"), ("baz", "qux")].iter().cloned()).await.unwrap();

            let m = b.h_get_all("foo").await.unwrap();
            assert_eq!(m.len(), 2);
            assert_eq!(Some(&"baz".into()), m.get("bar".as_bytes()));
            assert_eq!(Some(&"qux".into()), m.get("baz".as_bytes()));
        }

        #[tokio::test]
        #[serial]
        async fn test_z_range_by_score() {
            let b = $f().await;

            b.z_add("foo", "-2", -2.0).await.unwrap();
            b.z_add("foo", "-1", -1.0).await.unwrap();
            b.z_add("foo", "-0.5", -0.5).await.unwrap();
            b.z_add("foo", "0", 0.0).await.unwrap();
            b.z_add("foo", "0.5", 0.5).await.unwrap();
            b.z_add("foo", "0.5b", 0.5).await.unwrap();
            b.z_add("foo", "1", 1.0).await.unwrap();
            b.z_add("foo", "2", 2.0).await.unwrap();

            // MinMax
            let members = b.z_range_by_score("foo", -0.5, 1.0, 0).await.unwrap();
            assert_eq!(vec!["-0.5", "0", "0.5", "0.5b", "1"], members);

            // Limit
            let members = b.z_range_by_score("foo", -0.5, 1.0, 2).await.unwrap();
            assert_eq!(vec!["-0.5", "0"], members);

            // -Inf
            let members = b.z_range_by_score("foo", f64::NEG_INFINITY, 1.0, 0).await.unwrap();
            assert_eq!(vec!["-2", "-1", "-0.5", "0", "0.5", "0.5b", "1"], members);

            // +Inf
            let members = b.z_range_by_score("foo", -0.5, f64::INFINITY, 0).await.unwrap();
            assert_eq!(vec!["-0.5", "0", "0.5", "0.5b", "1", "2"], members);

            // Rev
            {
                // MinMax
                let members = b.z_rev_range_by_score("foo", -0.5, 1.0, 0).await.unwrap();
                assert_eq!(vec!["1", "0.5b", "0.5", "0", "-0.5"], members);

                // Limit
                let members = b.z_rev_range_by_score("foo", -0.5, 1.0, 2).await.unwrap();
                assert_eq!(vec!["1", "0.5b"], members);

                // -Inf
                let members = b.z_rev_range_by_score("foo", f64::NEG_INFINITY, 1.0, 0).await.unwrap();
                assert_eq!(vec!["1", "0.5b", "0.5", "0", "-0.5", "-1", "-2"], members);

                // +Inf
                let members = b.z_rev_range_by_score("foo", -0.5, f64::INFINITY, 0).await.unwrap();
                assert_eq!(vec!["2", "1", "0.5b", "0.5", "0", "-0.5"], members);
            }

            // Update
            {
                b.z_add("update-test", "foo", 2.0).await.unwrap();

                let members = b.z_range_by_score("update-test", 1.5, 2.5, 0).await.unwrap();
                assert_eq!(vec!["foo"], members);

                b.z_add("update-test", "foo", 3.0).await.unwrap();

                let members = b.z_range_by_score("update-test", 1.5, 2.5, 0).await.unwrap();
                assert_eq!(members.is_empty(), true);

                let members = b.z_range_by_score("update-test", 2.5, 3.5, 0).await.unwrap();
                assert_eq!(vec!["foo"], members);
            }
        }

        #[tokio::test]
        #[serial]
        async fn test_z_range_by_lex() {
            let b = $f().await;

            b.z_add("foo", "a", 0.0).await.unwrap();
            b.z_add("foo", "b", 0.0).await.unwrap();
            b.z_add("foo", "c", 0.0).await.unwrap();
            b.z_add("foo", "d", 0.0).await.unwrap();

            // Inf
            let members = b
                .z_range_by_lex("foo", Bound::<&str>::Unbounded, Bound::<&str>::Unbounded, 0)
                .await
                .unwrap();
            assert_eq!(vec!["a", "b", "c", "d"], members);

            /*
            // MinGreaterThanMax
            let members = b.z_range_by_lex("foo", Bound::Excluded("d"), Bound::Excluded("a"), 0).await.unwrap();
            assert_eq!(members.is_empty(), true);
            */

            // MinMaxExclusive
            let members = b.z_range_by_lex("foo", Bound::Excluded("a"), Bound::Excluded("d"), 0).await.unwrap();
            assert_eq!(vec!["b", "c"], members);

            /*
            // MinMaxInclusive
            let members = b.z_range_by_lex("foo", Bound::Included("a"), Bound::Included("d"), 0).await.unwrap();
            assert_eq!(vec!["a", "b", "c", "d"], members);

            // RangeInclusive
            let members = b.z_range_by_lex("foo", Bound::Included("b"), Bound::Included("c"), 0).await.unwrap();
            assert_eq!(vec!["b", "c"], members);

            // SingleElement
            let members = b.z_range_by_lex("foo", Bound::Included("b"), Bound::Included("b"), 0).await.unwrap();
            assert_eq!(vec!["b"], members);

            // SingleAbsentElement
            let members = b.z_range_by_lex("foo", Bound::Included("z"), Bound::Included("z"), 1).await.unwrap();
            assert_eq!(members.is_empty(), true);

            // Rev
            {
                // Inf
                let members = b
                    .z_rev_range_by_lex("foo", Bound::<&str>::Unbounded, Bound::<&str>::Unbounded, 0)
                    .await
                    .unwrap();
                assert_eq!(vec!["d", "c", "b", "a"], members);

                // MinMaxExclusive
                let members = b.z_rev_range_by_lex("foo", Bound::Excluded("a"), Bound::Excluded("d"), 0).await.unwrap();
                assert_eq!(vec!["c", "b"], members);

                // MinMaxInclusive
                let members = b.z_rev_range_by_lex("foo", Bound::Included("a"), Bound::Included("d"), 0).await.unwrap();
                assert_eq!(vec!["d", "c", "b", "a"], members);

                // RangeInclusive
                let members = b.z_rev_range_by_lex("foo", Bound::Included("b"), Bound::Included("c"), 0).await.unwrap();
                assert_eq!(vec!["c", "b"], members);

                // SingleAbsentElement
                let members = b.z_rev_range_by_lex("foo", Bound::Included("z"), Bound::Included("z"), 1).await.unwrap();
                assert_eq!(members.is_empty(), true);
            }
            */
        }

        #[tokio::test]
        #[serial]
        async fn test_z_rem() {
            let b = $f().await;

            b.z_add("foo", "a", 0.0).await.unwrap();
            b.z_add("foo", "b", 1.0).await.unwrap();

            let members = b.z_range_by_score("foo", 0.0, 10.0, 0).await.unwrap();
            assert_eq!(vec!["a", "b"], members);

            b.z_rem("foo", "a").await.unwrap();

            let members = b.z_range_by_score("foo", 0.0, 10.0, 0).await.unwrap();
            assert_eq!(vec!["b"], members);
        }

        #[tokio::test]
        #[serial]
        async fn test_zh_range_by_score() {
            let b = $f().await;

            b.zh_add("foo", "a", "-2", -2.0).await.unwrap();
            b.zh_add("foo", "b", "-1", -1.0).await.unwrap();
            b.zh_add("foo", "c", "-0.5", -0.5).await.unwrap();
            b.zh_add("foo", "d", "0", 0.0).await.unwrap();
            b.zh_add("foo", "e", "0.5", 0.5).await.unwrap();
            b.zh_add("foo", "f", "0.5b", 0.5).await.unwrap();
            b.zh_add("foo", "g", "1", 1.0).await.unwrap();
            b.zh_add("foo", "h", "2", 2.0).await.unwrap();

            // MinMax
            let members = b.zh_range_by_score("foo", -0.5, 1.0, 0).await.unwrap();
            assert_eq!(vec!["-0.5", "0", "0.5", "0.5b", "1"], members);

            // Limit
            let members = b.zh_range_by_score("foo", -0.5, 1.0, 2).await.unwrap();
            assert_eq!(vec!["-0.5", "0"], members);

            // -Inf
            let members = b.zh_range_by_score("foo", f64::NEG_INFINITY, 1.0, 0).await.unwrap();
            assert_eq!(vec!["-2", "-1", "-0.5", "0", "0.5", "0.5b", "1"], members);

            // +Inf
            let members = b.zh_range_by_score("foo", -0.5, f64::INFINITY, 0).await.unwrap();
            assert_eq!(vec!["-0.5", "0", "0.5", "0.5b", "1", "2"], members);

            // Rev
            {
                // MinMax
                let members = b.zh_rev_range_by_score("foo", -0.5, 1.0, 0).await.unwrap();
                assert_eq!(vec!["1", "0.5b", "0.5", "0", "-0.5"], members);

                // Limit
                let members = b.zh_rev_range_by_score("foo", -0.5, 1.0, 2).await.unwrap();
                assert_eq!(vec!["1", "0.5b"], members);

                // -Inf
                let members = b.zh_rev_range_by_score("foo", f64::NEG_INFINITY, 1.0, 0).await.unwrap();
                assert_eq!(vec!["1", "0.5b", "0.5", "0", "-0.5", "-1", "-2"], members);

                // +Inf
                let members = b.zh_rev_range_by_score("foo", -0.5, f64::INFINITY, 0).await.unwrap();
                assert_eq!(vec!["2", "1", "0.5b", "0.5", "0", "-0.5"], members);
            }

            // ZAddMigration
            {
                b.z_add("zaddtest", "a", 0.0).await.unwrap();
                b.zh_add("zaddtest", "b", "bob", 0.0).await.unwrap();
                b.z_add("zaddtest", "c", 0.0).await.unwrap();
                b.zh_add("zaddtest", "d", "dan", 0.0).await.unwrap();

                let members = b.zh_range_by_score("zaddtest", -0.5, 1.0, 0).await.unwrap();
                assert_eq!(vec!["a", "bob", "c", "dan"], members);
            }

            // Update
            {
                b.zh_add("update-test", "f", "foo", 2.0).await.unwrap();

                let members = b.zh_range_by_score("update-test", 1.5, 2.5, 0).await.unwrap();
                assert_eq!(vec!["foo"], members);

                b.zh_add("update-test", "f", "foo", 3.0).await.unwrap();

                let members = b.zh_range_by_score("update-test", 1.5, 2.5, 0).await.unwrap();
                assert_eq!(members.is_empty(), true);

                let members = b.zh_range_by_score("update-test", 2.5, 3.5, 0).await.unwrap();
                assert_eq!(vec!["foo"], members);
            }
        }

        #[tokio::test]
        #[serial]
        async fn test_z_count() {
            let b = $f().await;

            b.z_add("foo", "a", 0.0).await.unwrap();
            b.z_add("foo", "b", 1.0).await.unwrap();
            b.z_add("foo", "c", 2.0).await.unwrap();
            b.z_add("foo", "d", 3.0).await.unwrap();
            b.z_add("foo", "e", 4.0).await.unwrap();
            b.z_add("foo", "f", 5.0).await.unwrap();

            assert_eq!(b.z_count("foo", 1.0, 2.0).await.unwrap(), 2);
            assert_eq!(b.z_count("foo", 1.0, 1.5).await.unwrap(), 1);
            assert_eq!(b.z_count("foo", f64::NEG_INFINITY, 2.0).await.unwrap(), 3);
            assert_eq!(b.z_count("foo", f64::NEG_INFINITY, f64::INFINITY).await.unwrap(), 6);
            assert_eq!(b.z_count("foo", 2.0, f64::INFINITY).await.unwrap(), 4);

            // DynamoDB has to paginate requests for z_counts on big sets.
            let mut big_value = Vec::new();
            big_value.resize(1000, 'x' as u8);
            for i in 0..1100 {
                b.z_add("big", [i.to_string().as_bytes().to_vec(), big_value.clone()].concat(), 0.0)
                    .await
                    .unwrap();
            }
            assert_eq!(b.z_count("big", 0.0, 0.0).await.unwrap(), 1100);
        }

        #[tokio::test]
        #[serial]
        async fn test_zh_count() {
            let b = $f().await;

            b.zh_add("foo", "a", "a", 0.0).await.unwrap();
            b.zh_add("foo", "b", "b", 1.0).await.unwrap();
            b.zh_add("foo", "c", "c", 2.0).await.unwrap();
            b.zh_add("foo", "d", "d", 3.0).await.unwrap();
            b.zh_add("foo", "e", "e", 4.0).await.unwrap();
            b.zh_add("foo", "f", "f", 5.0).await.unwrap();

            assert_eq!(b.zh_count("foo", 1.0, 2.0).await.unwrap(), 2);
            assert_eq!(b.zh_count("foo", 1.0, 1.5).await.unwrap(), 1);
            assert_eq!(b.zh_count("foo", f64::NEG_INFINITY, 2.0).await.unwrap(), 3);
            assert_eq!(b.zh_count("foo", f64::NEG_INFINITY, f64::INFINITY).await.unwrap(), 6);
            assert_eq!(b.zh_count("foo", 2.0, f64::INFINITY).await.unwrap(), 4);

            // DynamoDB has to paginate requests for zh_counts on big sets.
            let mut big_value = Vec::new();
            big_value.resize(1000, 'x' as u8);
            for i in 0..1100 {
                b.z_add("big", [i.to_string().as_bytes().to_vec(), big_value.clone()].concat(), 0.0)
                    .await
                    .unwrap();
            }
            assert_eq!(b.zh_count("big", 0.0, 0.0).await.unwrap(), 1100);
        }

        #[tokio::test]
        #[serial]
        async fn test_batch_get() {
            let b = $f().await;

            b.set("foo", "bar").await.unwrap();
            b.set("foo2", "bar2").await.unwrap();

            let mut batch = BatchOperation::new();
            let get = batch.get("foo");
            b.exec_batch(batch).await.unwrap();

            assert_eq!(get.value(), Some("bar".into()));

            let mut batch = BatchOperation::new();
            let get = batch.get("foo");
            let get2 = batch.get("foo2");
            let get3 = batch.get("foo3");
            b.exec_batch(batch).await.unwrap();

            assert_eq!(get.value(), Some("bar".into()));
            assert_eq!(get2.value(), Some("bar2".into()));
            assert_eq!(get3.value(), None);
        }

        #[tokio::test]
        #[serial]
        async fn test_atomic_write_set() {
            let b = $f().await;

            let mut tx = AtomicWriteOperation::new();
            tx.set("foo", "bar");
            tx.set("bar", "baz");
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), true);

            let mut tx = AtomicWriteOperation::new();
            tx.set_nx("foo", "bar");
            tx.set("bar", "baz");
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), false);

            let mut tx = AtomicWriteOperation::new();
            let c = tx.set_nx("foo", "bar");
            tx.set("bar", "baz");
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), false);
            assert_eq!(c.failed(), true);
        }

        #[tokio::test]
        #[serial]
        async fn test_atomic_write_set_nx() {
            let b = $f().await;

            b.set("foo", "bar").await.unwrap();

            let mut tx = AtomicWriteOperation::new();
            let c = tx.set_nx("foo", "bar");
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), false);
            assert_eq!(c.failed(), true);

            let mut tx = AtomicWriteOperation::new();
            let c1 = tx.set_nx("notset", "bar");
            let c2 = tx.set_nx("notset2", "bar2");
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), true);
            assert_eq!(c1.failed(), false);
            assert_eq!(c2.failed(), false);

            assert_eq!(b.get("notset").await.unwrap(), Some("bar".into()));
            assert_eq!(b.get("notset2").await.unwrap(), Some("bar2".into()));
        }

        #[tokio::test]
        #[serial]
        async fn test_atomic_write_delete() {
            let b = $f().await;

            b.set("foo", "bar").await.unwrap();
            b.set("deleteme", "bar").await.unwrap();

            let mut tx = AtomicWriteOperation::new();
            let c = tx.set_nx("foo", "bar");
            tx.delete("deleteme");
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), false);
            assert_eq!(c.failed(), true);

            assert_eq!(b.get("deleteme").await.unwrap(), Some("bar".into()));

            let mut tx = AtomicWriteOperation::new();
            let c = tx.set_nx("notset", "bar");
            tx.delete("deleteme");
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), true);
            assert_eq!(c.failed(), false);

            assert_eq!(b.get("deleteme").await.unwrap(), None);
        }

        #[tokio::test]
        #[serial]
        async fn test_atomic_write_delete_xx() {
            let b = $f().await;

            b.set("foo", "bar").await.unwrap();
            b.set("deleteme", "bar").await.unwrap();

            let mut tx = AtomicWriteOperation::new();
            let c = tx.delete_xx("notset");
            tx.delete("deleteme");
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), false);
            assert_eq!(c.failed(), true);

            assert_eq!(b.get("deleteme").await.unwrap(), Some("bar".into()));

            let mut tx = AtomicWriteOperation::new();
            let c = tx.delete_xx("foo");
            tx.delete("deleteme");
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), true);
            assert_eq!(c.failed(), false);

            assert_eq!(b.get("deleteme").await.unwrap(), None);
        }

        #[tokio::test]
        #[serial]
        async fn test_atomic_write_z_add() {
            let b = $f().await;

            b.set("zsetcond", "foo").await.unwrap();

            let mut tx = AtomicWriteOperation::new();
            let c = tx.set_nx("zsetcond", "foo");
            tx.z_add("zset", "foo", 1.0);
            tx.z_add("zset", "bar", 2.0);
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), false);
            assert_eq!(c.failed(), true);

            assert_eq!(b.z_count("zset", 0.0, 10.0).await.unwrap(), 0);

            let mut tx = AtomicWriteOperation::new();
            tx.z_add("zset", "foo", 1.0);
            tx.z_add("zset", "bar", 2.0);
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), true);

            assert_eq!(b.z_count("zset", 0.0, 10.0).await.unwrap(), 2);

            let mut tx = AtomicWriteOperation::new();
            tx.z_rem("zset", "foo");
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), true);

            assert_eq!(b.z_count("zset", 0.0, 10.0).await.unwrap(), 1);
        }

        #[tokio::test]
        #[serial]
        async fn test_atomic_write_zh_add() {
            let b = $f().await;

            b.set("zhashcond", "foo").await.unwrap();

            let mut tx = AtomicWriteOperation::new();
            let c = tx.set_nx("zhashcond", "foo");
            tx.zh_add("zhash", "f", "foo", 1.0);
            tx.zh_add("zhash", "b", "bar", 2.0);
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), false);
            assert_eq!(c.failed(), true);

            assert_eq!(b.zh_count("zhash", 0.0, 10.0).await.unwrap(), 0);

            let mut tx = AtomicWriteOperation::new();
            tx.zh_add("zhash", "f", "foo", 1.0);
            tx.zh_add("zhash", "b", "bar", 2.0);
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), true);

            assert_eq!(b.zh_count("zhash", 0.0, 10.0).await.unwrap(), 2);

            // ZHRem
            {
                let mut tx = AtomicWriteOperation::new();
                tx.zh_rem("zhash", "f");
                assert_eq!(b.exec_atomic_write(tx).await.unwrap(), true);

                assert_eq!(b.zh_count("zhash", 0.0, 10.0).await.unwrap(), 1);
            }
        }

        #[tokio::test]
        #[serial]
        async fn test_atomic_write_s_add() {
            let b = $f().await;

            b.set("setcond", "foo").await.unwrap();

            let mut tx = AtomicWriteOperation::new();
            let c = tx.set_nx("setcond", "foo");
            tx.s_add("set", "foo");
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), false);
            assert_eq!(c.failed(), true);

            let members = b.s_members("set").await.unwrap();
            assert_eq!(members.is_empty(), true);

            let mut tx = AtomicWriteOperation::new();
            tx.s_add("set", "foo");
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), true);

            assert_eq!(vec!["foo"], b.s_members("set").await.unwrap());

            b.s_add("set", "bar").await.unwrap();

            let mut tx = AtomicWriteOperation::new();
            tx.s_rem("set", "foo");
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), true);

            let members = b.s_members("set").await.unwrap();
            assert_eq!(vec!["bar"], members);
        }

        #[tokio::test]
        #[serial]
        async fn test_atomic_write_h_set() {
            let b = $f().await;

            b.set("setcond", "foo").await.unwrap();

            let mut tx = AtomicWriteOperation::new();
            let c = tx.set_nx("setcond", "foo");
            tx.h_set("h", [("foo", "bar")].iter().cloned());
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), false);
            assert_eq!(c.failed(), true);

            let v = b.h_get("h", "foo").await.unwrap();
            assert_eq!(v, None);

            let mut tx = AtomicWriteOperation::new();
            tx.h_set("h", [("foo", "bar")].iter().cloned());
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), true);

            let v = b.h_get("h", "foo").await.unwrap();
            assert_eq!(Some("bar".into()), v);
        }

        #[tokio::test]
        #[serial]
        async fn test_atomic_write_h_set_nx() {
            let b = $f().await;

            let mut tx = AtomicWriteOperation::new();
            tx.set("foo", "bar");
            let c = tx.h_set_nx("h", "foo", "bar");
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), true);
            assert_eq!(c.failed(), false);

            let v = b.get("foo").await.unwrap();
            assert_eq!(v, Some("bar".into()));

            let mut tx = AtomicWriteOperation::new();
            tx.set("foo", "baz");
            let c = tx.h_set_nx("h", "foo", "bar");
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), false);
            assert_eq!(c.failed(), true);

            let v = b.get("foo").await.unwrap();
            assert_eq!(v, Some("bar".into()));
        }

        #[tokio::test]
        #[serial]
        async fn test_atomic_write_h_del() {
            let b = $f().await;

            b.set("setcond", "foo").await.unwrap();
            b.h_set("h", [("foo", "bar")].iter().cloned()).await.unwrap();

            let mut tx = AtomicWriteOperation::new();
            let c = tx.set_nx("setcond", "foo");
            tx.h_del("h", ["foo"].iter().cloned());
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), false);
            assert_eq!(c.failed(), true);

            let v = b.h_get("h", "foo").await.unwrap();
            assert_eq!(Some("bar".into()), v);

            let mut tx = AtomicWriteOperation::new();
            tx.h_del("h", ["foo"].iter().cloned());
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), true);

            let v = b.h_get("h", "foo").await.unwrap();
            assert_eq!(v, None);
        }
    };
}
