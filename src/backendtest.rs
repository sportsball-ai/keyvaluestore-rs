#[macro_export]
macro_rules! test_backend {
    ($f:expr) => {
        use crate::{AtomicWriteOperation, Backend, BatchOperation};

        #[tokio::test]
        #[serial]
        async fn test_set() {
            let b = $f();

            b.set("foo", "bar").await.unwrap();
            assert_eq!(b.get("foo").await.unwrap(), Some("bar".into()));
        }

        #[tokio::test]
        #[serial]
        async fn test_delete() {
            let b = $f();

            assert_eq!(b.delete("foo").await.unwrap(), false);

            b.set("foo", "bar").await.unwrap();
            assert_eq!(b.get("foo").await.unwrap(), Some("bar".into()));

            assert_eq!(b.delete("foo").await.unwrap(), true);
            assert_eq!(b.get("foo").await.unwrap(), None);
        }

        #[tokio::test]
        #[serial]
        async fn test_set_nx() {
            let b = $f();

            assert_eq!(b.set_nx("foo", "bar").await.unwrap(), true);
            assert_eq!(b.get("foo").await.unwrap(), Some("bar".into()));

            assert_eq!(b.set_nx("foo", "bar").await.unwrap(), false);
        }

        #[tokio::test]
        #[serial]
        async fn test_set_eq() {
            let b = $f();

            b.set("foo", "bar").await.unwrap();
            assert_eq!(b.set_eq("foo", "baz", "bar").await.unwrap(), true);
            assert_eq!(b.get("foo").await.unwrap(), Some("baz".into()));

            assert_eq!(b.set_eq("foo", "qux", "bar").await.unwrap(), false);
            assert_eq!(b.get("foo").await.unwrap(), Some("baz".into()));
        }

        #[tokio::test]
        #[serial]
        async fn test_s_add() {
            let b = $f();

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
        async fn test_z_range_by_score() {
            let b = $f();

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
        async fn test_z_count() {
            let b = $f();

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
        async fn test_batch_get() {
            let b = $f();

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
            let b = $f();

            let mut tx = AtomicWriteOperation::new();
            tx.set("foo", "bar");
            tx.set("foo", "baz");
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), true);

            let mut tx = AtomicWriteOperation::new();
            tx.set_nx("foo", "bar");
            tx.set("foo", "baz");
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), false);

            let mut tx = AtomicWriteOperation::new();
            let c = tx.set_nx("foo", "bar");
            tx.set("foo", "baz");
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), false);
            assert_eq!(c.failed(), true);
        }

        #[tokio::test]
        #[serial]
        async fn test_atomic_write_set_nx() {
            let b = $f();

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
            let b = $f();

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
            let b = $f();

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
            let b = $f();

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
        async fn test_atomic_write_s_add() {
            let b = $f();

            b.set("setcond", "foo").await.unwrap();

            let mut tx = AtomicWriteOperation::new();
            let c = tx.set_nx("setcond", "foo");
            tx.s_add("set", "foo");
            tx.s_add("set", "bar");
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), false);
            assert_eq!(c.failed(), true);

            let members = b.s_members("set").await.unwrap();
            assert_eq!(members.is_empty(), true);

            let mut tx = AtomicWriteOperation::new();
            tx.s_add("set", "foo");
            tx.s_add("set", "bar");
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), true);

            let mut members = b.s_members("set").await.unwrap();
            members.sort();
            assert_eq!(vec!["bar", "foo"], members);

            let mut tx = AtomicWriteOperation::new();
            tx.s_rem("set", "foo");
            assert_eq!(b.exec_atomic_write(tx).await.unwrap(), true);

            let members = b.s_members("set").await.unwrap();
            assert_eq!(vec!["bar"], members);
        }
    };
}
