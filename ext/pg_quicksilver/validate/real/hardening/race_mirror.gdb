# race_mirror.gdb: attached to a backend (see race_mirror.sh). Stops it between the mirror
# existence check and the names it looks up again (pg_quicksilver.c, qs_build_mirror), makes the
# mirror view disappear as a standby's replay would, and lets the backend see the invalidation.
set pagination off
set confirm off
break pg_quicksilver.c:908 if m->heap == HEAPOID
continue
shell $RACE_DROP
call (void) AcceptInvalidationMessages()
continue
bt 8
quit
