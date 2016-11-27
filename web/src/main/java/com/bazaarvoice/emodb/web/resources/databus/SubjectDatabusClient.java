package com.bazaarvoice.emodb.web.resources.databus;

import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.databus.api.AuthDatabus;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.client.DatabusAuthenticator;

import static com.google.common.base.Preconditions.checkNotNull;

public class SubjectDatabusClient extends AbstractSubjectDatabus {

    private final AuthDatabus _authDatabus;

    public SubjectDatabusClient(AuthDatabus authDatabus) {
        _authDatabus = checkNotNull(authDatabus, "authDatabus");
    }

    @Override
    protected Databus databus(Subject subject) {
        return DatabusAuthenticator.proxied(_authDatabus).usingCredentials(subject.getId());
    }

    AuthDatabus getClient() {
        return _authDatabus;
    }
}
