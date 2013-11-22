/* $OpenLDAP: pkg/ldap/servers/slapd/back-perl/add.c,v 1.20.2.6 2011/01/04 23:50:41 kurt Exp $ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 1999-2011 The OpenLDAP Foundation.
 * Portions Copyright 1999 John C. Quillan.
 * Portions Copyright 2002 myinternet Limited.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */

#include "perl_back.h"

int
perl_back_add(
	Operation	*op,
	SlapReply	*rs )
{
	PerlBackend *perl_back = (PerlBackend *) op->o_bd->be_private;
	int len;
	int count;

#if defined(HAVE_WIN32_ASPERL) || defined(USE_ITHREADS)
	PERL_SET_CONTEXT( PERL_INTERPRETER );
#endif
	ldap_pvt_thread_mutex_lock( &perl_interpreter_mutex );
	ldap_pvt_thread_mutex_lock( &entry2str_mutex );

	{
		dSP; ENTER; SAVETMPS;

		PUSHMARK(sp);
		XPUSHs( perl_back->pb_obj_ref );
		XPUSHs(sv_2mortal(newSVpv( entry2str( op->ora_e, &len ), 0 )));

		PUTBACK;

#ifdef PERL_IS_5_6
		count = call_method("add", G_SCALAR);
#else
		count = perl_call_method("add", G_SCALAR);
#endif

		SPAGAIN;

		if (count != 1) {
			croak("Big trouble in back_add\n");
		}
							 
		rs->sr_err = POPi;

		PUTBACK; FREETMPS; LEAVE;
	}

	ldap_pvt_thread_mutex_unlock( &entry2str_mutex );
	ldap_pvt_thread_mutex_unlock( &perl_interpreter_mutex );	

	send_ldap_result( op, rs );

	Debug( LDAP_DEBUG_ANY, "Perl ADD\n", 0, 0, 0 );
	return( 0 );
}
