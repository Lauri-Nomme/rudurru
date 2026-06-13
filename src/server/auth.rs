use crate::proto::etcdserverpb;
use tonic::{Request, Response, Status};

#[derive(Debug, Default)]
pub struct Auth;

#[tonic::async_trait]
impl etcdserverpb::auth_server::Auth for Auth {
    async fn auth_enable(
        &self,
        _req: Request<etcdserverpb::AuthEnableRequest>,
    ) -> Result<Response<etcdserverpb::AuthEnableResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn auth_disable(
        &self,
        _req: Request<etcdserverpb::AuthDisableRequest>,
    ) -> Result<Response<etcdserverpb::AuthDisableResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn authenticate(
        &self,
        _req: Request<etcdserverpb::AuthenticateRequest>,
    ) -> Result<Response<etcdserverpb::AuthenticateResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn user_add(
        &self,
        _req: Request<etcdserverpb::AuthUserAddRequest>,
    ) -> Result<Response<etcdserverpb::AuthUserAddResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn user_get(
        &self,
        _req: Request<etcdserverpb::AuthUserGetRequest>,
    ) -> Result<Response<etcdserverpb::AuthUserGetResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn user_list(
        &self,
        _req: Request<etcdserverpb::AuthUserListRequest>,
    ) -> Result<Response<etcdserverpb::AuthUserListResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn user_delete(
        &self,
        _req: Request<etcdserverpb::AuthUserDeleteRequest>,
    ) -> Result<Response<etcdserverpb::AuthUserDeleteResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn user_change_password(
        &self,
        _req: Request<etcdserverpb::AuthUserChangePasswordRequest>,
    ) -> Result<Response<etcdserverpb::AuthUserChangePasswordResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn user_grant_role(
        &self,
        _req: Request<etcdserverpb::AuthUserGrantRoleRequest>,
    ) -> Result<Response<etcdserverpb::AuthUserGrantRoleResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn user_revoke_role(
        &self,
        _req: Request<etcdserverpb::AuthUserRevokeRoleRequest>,
    ) -> Result<Response<etcdserverpb::AuthUserRevokeRoleResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn role_add(
        &self,
        _req: Request<etcdserverpb::AuthRoleAddRequest>,
    ) -> Result<Response<etcdserverpb::AuthRoleAddResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn role_get(
        &self,
        _req: Request<etcdserverpb::AuthRoleGetRequest>,
    ) -> Result<Response<etcdserverpb::AuthRoleGetResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn role_list(
        &self,
        _req: Request<etcdserverpb::AuthRoleListRequest>,
    ) -> Result<Response<etcdserverpb::AuthRoleListResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn role_delete(
        &self,
        _req: Request<etcdserverpb::AuthRoleDeleteRequest>,
    ) -> Result<Response<etcdserverpb::AuthRoleDeleteResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn role_grant_permission(
        &self,
        _req: Request<etcdserverpb::AuthRoleGrantPermissionRequest>,
    ) -> Result<Response<etcdserverpb::AuthRoleGrantPermissionResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn role_revoke_permission(
        &self,
        _req: Request<etcdserverpb::AuthRoleRevokePermissionRequest>,
    ) -> Result<Response<etcdserverpb::AuthRoleRevokePermissionResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }
}
