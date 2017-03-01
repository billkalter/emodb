package com.bazaarvoice.emodb.web.resources.uac;

import io.dropwizard.jersey.params.AbstractParam;

/**
 * Emo roles support an optional group attribute.  However, the REST path for accessing roles is of the form
 * <code>{group}/{id}</code>.  Since there is no way to express <code>null</code> in a path the underbar character
 * is used instead.  This is also the convention used for role-permissions by
 * {@link com.bazaarvoice.emodb.web.auth.Permissions#toRoleGroupResource(String)} so keeping with that convention
 * makes permission checking for null groups simple.
 */
public class GroupParam extends AbstractParam<String> {

    public GroupParam(String input) {
        super(input);
    }

    @Override
    protected String parse(String input) throws Exception {
        return "_".equals(input) ? null : input;
    }
}
