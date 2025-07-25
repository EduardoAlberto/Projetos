from typing import List

class AccessController:
    def __init__(self,spark):
        # Usuários fictícios com permissões simuladas
        self.user_db = {
            "admin": {"roles": ["read", "write", "delete"]},
            "analista": {"roles": ["read", "write"]},
            "visitante": {"roles": ["read"]},
        }

    def is_valid_user(self, user_id: str) -> bool:
        """Verifica se o usuário existe no sistema"""
        return user_id in self.user_db

    def get_roles(self, user_id: str) -> List[str]:
        """Retorna as permissões do usuário"""
        if self.is_valid_user(user_id):
            return self.user_db[user_id]["roles"]
        return []

    def has_permission(self, user_id: str, permission: str) -> bool:
        """Verifica se o usuário tem permissão específica"""
        return permission in self.get_roles(user_id)

    def require_permission(self, user_id: str, permission: str):
        """Lança exceção se o usuário não tiver acesso"""
        if not self.has_permission(user_id, permission):
            raise PermissionError(f"Usuário '{user_id}' não tem permissão '{permission}'")
