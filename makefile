path=pwd
BtreeManager: test_assign4_1.c btree_mgr.c record_mgr.c expr.c rm_serializer.c buffer_mgr.c buffer_mgr_stat.c storage_mgr.c dberror.c
	gcc -o BtreeManager test_assign4_1.c btree_mgr.c record_mgr.c expr.c rm_serializer.c buffer_mgr.c buffer_mgr_stat.c storage_mgr.c dberror.c -I $(path)
clean:
	rm BtreeManager
