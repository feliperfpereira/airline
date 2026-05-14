package controllers

import com.patson.data.UserSource

import javax.inject.Inject
import play.api._
import play.api.libs.json.JsValue
import play.api.libs.streams.ActorFlow
import play.api.mvc._
import websocket.MyWebSocketActor

import scala.concurrent.Future

class WebsocketApplication @Inject()(cc: ControllerComponents) extends AbstractController(cc) {
  val logger = Logger(this.getClass)
  def wsWithActor = WebSocket.acceptOrResult[JsValue, JsValue] { request =>
    Future.successful {
      if (AuthenticationObject.singlePlayerEnabled) {
        UserSource.loadUserByUserName(AuthenticationObject.singlePlayerUsername) match {
          case Some(user) =>
            logger.info(s"single-player websocket accepted for user ${user.userName}")
            Right(ActorFlow.actorRef { out =>
              MyWebSocketActor.props(out, user.id, request.remoteAddress)
            })
          case None =>
            logger.warn(s"single-player user '${AuthenticationObject.singlePlayerUsername}' not found in DB")
            Left(Forbidden)
        }
      } else {
        request.session.get("userToken") match {
          case None =>
            logger.info(s"websocket rejected: no userToken cookie (ip=${request.remoteAddress})")
            Left(Forbidden)
          case Some(token) =>
            SessionUtil.getUserId(token) match {
              case None =>
                logger.info(s"websocket rejected: token not found or expired (ip=${request.remoteAddress} token=${token.take(20)}...)")
                Left(Forbidden)
              case Some(userId) =>
                logger.info("wsWithActor, client connected with userId " + userId)
                Right(ActorFlow.actorRef { out =>
                  println(s"userid $userId has actor ${out.path}")
                  MyWebSocketActor.props(out, userId, request.remoteAddress)
                })
            }
        }
      }
    }
  }
}
